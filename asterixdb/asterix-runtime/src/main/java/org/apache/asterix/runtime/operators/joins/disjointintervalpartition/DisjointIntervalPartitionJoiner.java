/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.runtime.operators.joins.disjointintervalpartition;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.runtime.operators.joins.AbstractJoiner;
import org.apache.asterix.runtime.operators.joins.IIntervalMergeJoinChecker;
import org.apache.asterix.runtime.operators.joins.intervalindex.TuplePrinterUtil;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.std.buffermanager.ITupleAccessor;
import org.apache.hyracks.dataflow.std.buffermanager.TupleAccessor;
import org.apache.hyracks.dataflow.std.buffermanager.VPartitionTupleBufferManager;
//import org.apache.hyracks.dataflow.common.io.RunFileReader;
//import org.apache.hyracks.dataflow.common.io.RunFileWriter;
import org.apache.hyracks.dataflow.std.join.RunFileReaderDir;
import org.apache.hyracks.dataflow.std.join.RunFileWriterDir;
import org.apache.hyracks.dataflow.std.structures.TuplePointer;

public class DisjointIntervalPartitionJoiner extends AbstractJoiner {

    private static final String BUILD_RUN_FILES_PREFIX = "disjointIntervalPartitionBuild";
    private static final String PROBE_RUN_FILES_PREFIX = "disjointIntervalPartitionProbe";
    private static final String SPILL_RUN_FILES_PREFIX = "disjointIntervalPartitionSpill";

    private static final Logger LOGGER = Logger.getLogger(DisjointIntervalPartitionJoiner.class.getName());

    private final DisjointIntervalPartitionAndSpill partitionAndSpill;
    private final int numberOfPartitions;
    private final LinkedList<RunFileReaderDir> leftRunFileReaders = new LinkedList<>();
    private final LinkedList<Long> leftPartitionCounts = new LinkedList<>();
    private final LinkedList<RunFileReaderDir> rightRunFileReaders = new LinkedList<>();
    private final LinkedList<Long> rightPartitionCounts = new LinkedList<>();

    private ITupleAccessor[] tupleAccessors;

    private final VSizeFrame tmpFrame;
    private final VSizeFrame tmpFrame2;
    private final ITupleAccessor joinTupleAccessor;

    private final IHyracksTaskContext ctx;

    private final RecordDescriptor leftRd;
    private final RecordDescriptor rightRd;

    private final IIntervalMergeJoinChecker imjc;

    private long inputFrameCount = 0;
    private long joinComparisonCount = 0;
    private long joinResultCount = 0;
    private long spillPartitionWriteCount = 0;
    private long spillPartitionReadCount = 0;
    private long spillJoinReadCount = 0;

    private final int partition;
    private final int memorySize;
    private DisjointIntervalPartitionComputer rightDipc;

    public DisjointIntervalPartitionJoiner(IHyracksTaskContext ctx, int memorySize, int partition,
            IIntervalMergeJoinChecker imjc, int leftKey, int rightKey, RecordDescriptor leftRd,
            RecordDescriptor rightRd, DisjointIntervalPartitionComputer leftDipc,
            DisjointIntervalPartitionComputer rightDipc) throws HyracksDataException {
        super(ctx, leftRd, rightRd);
        this.ctx = ctx;
        this.partition = partition;
        this.rightDipc = rightDipc;
        this.memorySize = memorySize;

        numberOfPartitions = memorySize - 1;
        tupleAccessors = new ITupleAccessor[numberOfPartitions];

        partitionAndSpill = new DisjointIntervalPartitionAndSpill(ctx, memorySize, numberOfPartitions);
        partitionAndSpill.init();
        partitionAndSpill.resetForNewDataset(leftRd, leftDipc, BUILD_RUN_FILES_PREFIX, getNewSpillWriter());

        this.rightRd = rightRd;
        this.leftRd = leftRd;
        this.imjc = imjc;

        tmpFrame = new VSizeFrame(ctx);
        tmpFrame2 = new VSizeFrame(ctx);
        joinTupleAccessor = new TupleAccessor(leftRd);
    }

    private RunFileWriterDir getNewSpillWriter() throws HyracksDataException {
        FileReference file = ctx.getJobletContext().createManagedWorkspaceFile(SPILL_RUN_FILES_PREFIX);
        RunFileWriterDir writer = new RunFileWriterDir(file, ctx.getIOManager());
        writer.open();
        return writer;
    }

    @Override
    public void processProbeFrame(ByteBuffer buffer, IFrameWriter writer) throws HyracksDataException {
        partitionAndSpill.processFrame(buffer);
        inputFrameCount++;
    }

    @Override
    public void processProbeClose(IFrameWriter writer) throws HyracksDataException {
        // Probe side
        if (partitionAndSpill.hasSpillPartitions()) {
            // Prepare spilled partitions for join
            getRunFileReaders(partitionAndSpill, rightRunFileReaders, rightPartitionCounts);
            processSpill(partitionAndSpill, rightRunFileReaders, rightPartitionCounts);
            processSpilledJoin(writer);
        } else {
            // Perform an in-memory join with LEFT spill files.
            getInMemoryTupleAccessors(partitionAndSpill, rightPartitionCounts);
            processInMemoryJoin(writer);
        }
        printPartitionCounts("Right", rightPartitionCounts);

        partitionAndSpill.close();
        resultAppender.write(writer, true);

        cleanupPartitions(leftRunFileReaders);
        cleanupPartitions(rightRunFileReaders);
        long cpu = joinComparisonCount;
        long io = spillJoinReadCount + inputFrameCount;
        if (LOGGER.isLoggable(Level.WARNING)) {
            LOGGER.warning(",DisjointIntervalPartitionJoiner Statistics Log," + partition + ",partition," + memorySize
                    + ",memory," + joinResultCount + ",results," + cpu + ",CPU," + io + ",IO,"
                    + spillPartitionWriteCount + ",partition_frames_written," + spillPartitionReadCount
                    + ",partition_frames_read," + spillJoinReadCount + ",partition_frames_read,"
                    + "?" + ",partition_comparison_left," + "?" 
                    + ",partition_comparison_right," + joinComparisonCount + ",join_comparison");
        }
        System.out.println(",DisjointIntervalPartitionJoiner Statistics Log," + partition + ",partition," + memorySize
                + ",memory," + joinResultCount + ",results," + cpu + ",CPU," + io + ",IO," + spillPartitionWriteCount
                + ",partition_frames_written," + spillPartitionReadCount + ",partition_frames_read,"
                + spillJoinReadCount + ",partition_frames_read," + "?" 
                + ",partition_comparison_left," + "?"  + ",partition_comparison_right,"
                + joinComparisonCount + ",join_comparison");
    }

    private void printPartitionCounts(String key, LinkedList<Long> counts) {
        System.err.println(key + " Partitions{");
        for (int i = 0; i < counts.size(); ++i) {
            System.err.println("  " + i + ": " + counts.get(i) + ",");
        }
        System.err.println("}");
    }

    @Override
    public void processBuildFrame(ByteBuffer buffer) throws HyracksDataException {
        partitionAndSpill.processFrame(buffer);
        inputFrameCount++;
    }

    @Override
    public void processBuildClose() throws HyracksDataException {
        getRunFileReaders(partitionAndSpill, leftRunFileReaders, leftPartitionCounts);
        // Handle spill file.
        processSpill(partitionAndSpill, leftRunFileReaders, leftPartitionCounts);

        printPartitionCounts("Left", leftPartitionCounts);

        // Probe side
        partitionAndSpill.resetForNewDataset(rightRd, rightDipc, PROBE_RUN_FILES_PREFIX, getNewSpillWriter());
    }

    private void processInMemoryJoin(IFrameWriter writer) throws HyracksDataException {
        //        int i = 0;
        for (int l = 0; l < leftRunFileReaders.size(); l++) {
            //            printRunFileTuples("spilled " + i++ + " on " + partition, leftRunFileReaders.get(l));
            resetInMemoryPartitions();
            leftRunFileReaders.get(l).reset();
            joinInMemoryPartitions(leftRunFileReaders.get(l), l, writer);
        }
    }

    private void joinInMemoryPartitions(RunFileReaderDir runFileReader, int leftPid, IFrameWriter writer)
            throws HyracksDataException {
        // Prepare frame.
        runFileReader.open();
        if (runFileReader.nextFrame(tmpFrame)) {
            joinTupleAccessor.reset(tmpFrame.getBuffer());
            joinTupleAccessor.next();
            spillJoinReadCount++;
        }
        while (joinTupleAccessor.exists()) {
            joinInMemoryPartitionTuple(leftPid, writer);
            loadNextTuple(joinTupleAccessor, runFileReader, tmpFrame);
        }
        runFileReader.close();
    }

    private void joinInMemoryPartitionTuple(int leftPid, IFrameWriter writer) throws HyracksDataException {
        for (int i = 0; i < tupleAccessors.length; i++) {
            while (null != tupleAccessors[i] && tupleAccessors[i].exists()) {
                // Join comparison
                //                printJoinDetails(leftPid, i, i);
                if (imjc.checkToSaveInResult(joinTupleAccessor, tupleAccessors[i])) {
                    addToResult(joinTupleAccessor, joinTupleAccessor.getTupleId(), tupleAccessors[i],
                            tupleAccessors[i].getTupleId(), false, writer);
                }
                joinComparisonCount++;

                // Load next item.
                if (imjc.checkToIncrementMerge(joinTupleAccessor, tupleAccessors[i])) {
                    // Still can compare this tuple. Do not advance partition.
                    break;
                } else {
                    tupleAccessors[i].next();
                }
            }
        }
    }

    private void resetInMemoryPartitions() {
        for (int i = 0; i < tupleAccessors.length; i++) {
            if (null != tupleAccessors[i]) {
                tupleAccessors[i].reset();
                tupleAccessors[i].next();
            }
        }
    }

    private void getInMemoryTupleAccessors(VPartitionTupleBufferManager buffer) {
        for (int i = 0; i < buffer.getNumPartitions(); i++) {
            if (buffer.getNumTuples(i) > 0) {
                tupleAccessors[i] = buffer.getPartitionFrameBufferManager(i).getTupleAccessor(rightRd);
                tupleAccessors[i].reset();
                tupleAccessors[i].next();
            } else {
                tupleAccessors[i] = null;
            }
        }
    }

    private void getInMemoryTupleAccessors(DisjointIntervalPartitionAndSpill dipas, LinkedList<Long> rpc) {
        for (int i = 0; i < numberOfPartitions; i++) {
            if (dipas.getPartitionSizeInTup(i) > 0) {
                rpc.add(dipas.getPartitionSizeInTup(i));
                tupleAccessors[i] = dipas.getPartitionTupleAccessor(i);
                tupleAccessors[i].reset();
                tupleAccessors[i].next();
            } else {
                tupleAccessors[i] = null;
            }
        }
    }

    private void processSpill(DisjointIntervalPartitionAndSpill dipas, LinkedList<RunFileReaderDir> rfrs,
            LinkedList<Long> rpc) throws HyracksDataException {
        while (dipas.getSpillSizeInTup() > 0) {
            RunFileReaderDir rfr = dipas.getSpillRFReader();
            dipas.reset(getNewSpillWriter());
            rfr.open();
            while (rfr.nextFrame(tmpFrame)) {
                dipas.processFrame(tmpFrame.getBuffer());
                spillPartitionReadCount++;
            }
            rfr.close();
            getRunFileReaders(dipas, rfrs, rpc);
        }
    }

    private void getRunFileReaders(DisjointIntervalPartitionAndSpill dipas, LinkedList<RunFileReaderDir> rfrs,
            LinkedList<Long> rpc) throws HyracksDataException {
        //        int offset = rfrs.size();
        dipas.spillAllPartitions();
        spillPartitionWriteCount += dipas.getSpillWriteCount();
        for (int i = 0; i < numberOfPartitions; i++) {
            if (dipas.getPartitionSizeInTup(i) > 0) {
                rfrs.add(dipas.getRFReader(i));
                rpc.add(dipas.getPartitionSizeInTup(i));
                //                printRunFileTuples("spilled " + (i + offset) + " on " + partition, dipas.getRFReader(i));
            } else {
                break;
            }
        }
    }

    private void printRunFileTuples(String message, RunFileReaderDir rfReader) throws HyracksDataException {
        rfReader.open();
        rfReader.reset();
        if (rfReader.nextFrame(tmpFrame)) {
            joinTupleAccessor.reset(tmpFrame.getBuffer());
            joinTupleAccessor.next();
        }
        while (joinTupleAccessor.exists()) {
            TuplePrinterUtil.printTuple("RunFile: " + message, joinTupleAccessor);
            loadNextTuple(joinTupleAccessor, rfReader, tmpFrame);
        }
        rfReader.close();
    }

    private void processSpilledJoin(IFrameWriter writer) throws HyracksDataException {
        VPartitionTupleBufferManager buffer =
                new VPartitionTupleBufferManager(ctx, VPartitionTupleBufferManager.NO_CONSTRAIN, numberOfPartitions,
                        numberOfPartitions * ctx.getInitialFrameSize());
        TuplePointer tpTemp = new TuplePointer();
        int partitionId = 0;
        TupleAccessor tupleAccessor = new TupleAccessor(rightRd);

        // Loop over all partitions from right (adding to memory)
        for (int i = 0; i < rightRunFileReaders.size(); i++) {
            // Add right partition to memory
            rightRunFileReaders.get(i).open();
            while (rightRunFileReaders.get(i).nextFrame(tmpFrame2)) {
                tupleAccessor.reset(tmpFrame2.getBuffer());
                tupleAccessor.next();
                while (tupleAccessor.exists()) {
                    if (!buffer.insertTuple(partitionId, tupleAccessor, tupleAccessor.getTupleId(), tpTemp)) {
                        // Memory has been filled. 
                        // Process tuples in memory with all left partitions.
                        getInMemoryTupleAccessors(buffer);
                        processInMemoryJoin(writer);
                        // reset memory
                        buffer.reset();
                        partitionId = 0;
                    } else {
                        tupleAccessor.next();
                    }
                }
                spillJoinReadCount++;
            }
            rightRunFileReaders.get(i).close();
            partitionId++;
        }
        getInMemoryTupleAccessors(buffer);
        processInMemoryJoin(writer);
    }

    private void cleanupPartitions(List<RunFileReaderDir> partitionRunsReaders) throws HyracksDataException {
        for (int i = 0; i < partitionRunsReaders.size(); i++) {
            partitionRunsReaders.get(i).close();
            partitionRunsReaders.get(i).delete();
        }
    }

    private void loadNextTuple(ITupleAccessor accessor, RunFileReaderDir reader, IFrame frame)
            throws HyracksDataException {
        accessor.next();
        if (!accessor.exists()) {
            // Load next frame.
            if (reader.nextFrame(frame)) {
                accessor.reset(frame.getBuffer());
                accessor.next();
                spillJoinReadCount++;
            }
        }
    }

    private void addToResult(IFrameTupleAccessor accessor1, int index1, IFrameTupleAccessor accessor2, int index2,
            boolean reversed, IFrameWriter writer) throws HyracksDataException {
        if (reversed) {
            FrameUtils.appendConcatToWriter(writer, resultAppender, accessor2, index2, accessor1, index1);
        } else {
            FrameUtils.appendConcatToWriter(writer, resultAppender, accessor1, index1, accessor2, index2);
        }
        joinResultCount++;
    }

    public void failureCleanUp() throws HyracksDataException {
        cleanupPartitions(leftRunFileReaders);
        cleanupPartitions(rightRunFileReaders);
    }

}

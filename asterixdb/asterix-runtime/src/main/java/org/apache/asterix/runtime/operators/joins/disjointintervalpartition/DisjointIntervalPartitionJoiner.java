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

import java.io.PrintStream;
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

    private static final String BUILD_PARTITION_FILES_PREFIX = "disjointIntervalPartitionBuild";
    private static final String PROBE_PARTITION_FILES_PREFIX = "disjointIntervalPartitionProbe";
    private static final String BUILD_FILE_PREFIX = "disjointIntervalPartitionJoinBuild";
    private static final String PROBE_FILE_PREFIX = "disjointIntervalPartitionJoinProbe";
    private static final String SPILL_RUN_FILES_PREFIX = "disjointIntervalPartitionSpill";

    private static final Logger LOGGER = Logger.getLogger(DisjointIntervalPartitionJoiner.class.getName());

    private final DisjointIntervalPartitionAndSpill partitionAndSpill;
    private final int numberOfPartitions;
    
    private final RunFileWriterDir leftRunFileWriter;
    private RunFileReaderDir leftRunFileReader;
    private final LinkedList<Long> leftPartitionCounts = new LinkedList<>();
    private final LinkedList<Long> leftPartitionOffsets = new LinkedList<>();
    
    private final RunFileWriterDir rightRunFileWriter;
    private RunFileReaderDir rightRunFileReader;
    private final LinkedList<Long> rightPartitionCounts = new LinkedList<>();
    private final LinkedList<Long> rightPartitionOffsets = new LinkedList<>();

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
    private long ioCumulativeTime = 0;
//    private String statsStream;

    private final int partition;
    private final int memorySize;
    private DisjointIntervalPartitionComputer rightDipc;

    public DisjointIntervalPartitionJoiner(IHyracksTaskContext ctx, int memorySize, int totalPartitions,
            IIntervalMergeJoinChecker imjc, int leftKey, int rightKey, RecordDescriptor leftRd,
            RecordDescriptor rightRd, DisjointIntervalPartitionComputer leftDipc,
            DisjointIntervalPartitionComputer rightDipc) throws HyracksDataException {
        super(ctx, leftRd, rightRd);
        this.ctx = ctx;
        this.partition = ctx.getTaskAttemptId().getTaskId().getPartition();
        this.rightDipc = rightDipc;
        this.memorySize = memorySize;

        int maxFilePerPartition = 1000 / totalPartitions;
        numberOfPartitions = (memorySize - 1 > maxFilePerPartition) ? maxFilePerPartition : memorySize - 1;
        //        numberOfPartitions = memorySize - 1;
        System.out.println("DisjointIntervalPartitionJoiner for partition " + partition + " using " + numberOfPartitions
                + " files");

        tupleAccessors = new ITupleAccessor[numberOfPartitions];

        partitionAndSpill = new DisjointIntervalPartitionAndSpill(ctx, memorySize, numberOfPartitions);
        partitionAndSpill.init();
        partitionAndSpill.resetForNewDataset(leftRd, leftDipc, BUILD_PARTITION_FILES_PREFIX, getNewSpillWriter());

        leftRunFileWriter = getNewSingleWriter(BUILD_FILE_PREFIX);
        rightRunFileWriter = getNewSingleWriter(PROBE_FILE_PREFIX);

        this.rightRd = rightRd;
        this.leftRd = leftRd;
        this.imjc = imjc;

        tmpFrame = new VSizeFrame(ctx);
        tmpFrame2 = new VSizeFrame(ctx);
        joinTupleAccessor = new TupleAccessor(leftRd);

//        statsStream = "";
    }

    private RunFileWriterDir getNewSpillWriter() throws HyracksDataException {
        return getNewSingleWriter(SPILL_RUN_FILES_PREFIX);
    }

    private RunFileWriterDir getNewSingleWriter(String prefix) throws HyracksDataException {
        FileReference file = ctx.getJobletContext().createManagedWorkspaceFile(prefix);
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
            getRunFileReaders(partitionAndSpill, rightPartitionCounts, rightRunFileWriter,
                    rightPartitionOffsets);
            processSpill(partitionAndSpill, rightPartitionCounts, rightRunFileWriter,
                    rightPartitionOffsets);
//            printPartitionCounts("Right", rightPartitionCounts);
//            printNumericList("Right Offsets", rightPartitionOffsets);
            rightRunFileReader = rightRunFileWriter.createReader();
            processSpilledJoin(writer);
        } else {
            // Perform an in-memory join with LEFT spill files.
            getInMemoryTupleAccessors(partitionAndSpill, rightPartitionCounts);
//            printPartitionCounts("Right", rightPartitionCounts);
//            printNumericList("Right Offsets", rightPartitionOffsets);
            processInMemoryJoin(writer);
        }

        partitionAndSpill.close();
        resultAppender.write(writer, true);

        failureCleanUp();

        long cpu = joinComparisonCount;
        long io = spillJoinReadCount + inputFrameCount;
        if (LOGGER.isLoggable(Level.WARNING)) {
            LOGGER.warning(",DisjointIntervalPartitionJoiner Statistics Log," + partition + ",partition," + memorySize
                    + ",memory," + joinResultCount + ",results," + cpu + ",CPU," + io + ",IO,"
                    + spillPartitionWriteCount + ",partition_frames_written," + spillPartitionReadCount
                    + ",partition_frames_read," + spillJoinReadCount + ",partition_frames_read," + "?"
                    + ",partition_comparison_left," + "?" + ",partition_comparison_right," + joinComparisonCount
                    + ",join_comparison, " + ioCumulativeTime + ",ioCumulativeTime");
        }
        System.out.println(",DisjointIntervalPartitionJoiner Statistics Log," + partition + ",partition," + memorySize
                + ",memory," + joinResultCount + ",results," + cpu + ",CPU," + io + ",IO," + spillPartitionWriteCount
                + ",partition_frames_written," + spillPartitionReadCount + ",partition_frames_read,"
                + spillJoinReadCount + ",partition_frames_read," + "?" + ",partition_comparison_left," + "?"
                + ",partition_comparison_right," + joinComparisonCount + ",join_comparison, " + ioCumulativeTime
                + ",ioCumulativeTime");

        System.out.println(
                ",DisjointIntervalPartitionJoiner Single File," + leftRunFileWriter.getFileSize() + ",size left");
        //        printNumericList("Left Offsets", leftPartitionOffsets);
        //        printNumericList("Right Offsets", rightPartitionOffsets);

    }

    private void printPartitionCounts(String key, LinkedList<Long> counts) {
        printNumericList(key + " Partitions", counts);
    }

    private void printNumericList(String key, LinkedList<Long> counts) {
        System.err.println(key + " {");
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
        getRunFileReaders(partitionAndSpill, leftPartitionCounts, leftRunFileWriter,
                leftPartitionOffsets);

        // Handle spill file.
        processSpill(partitionAndSpill, leftPartitionCounts, leftRunFileWriter,
                leftPartitionOffsets);

//        printPartitionCounts("Left", leftPartitionCounts);
//        printNumericList("Left Offsets", leftPartitionOffsets);

        leftRunFileReader = leftRunFileWriter.createReader();

        // Probe side
        partitionAndSpill.resetForNewDataset(rightRd, rightDipc, PROBE_PARTITION_FILES_PREFIX, getNewSpillWriter());
    }

    private void processInMemoryJoin(IFrameWriter writer) throws HyracksDataException {
        leftRunFileReader.open();
        resetInMemoryPartitions();
        leftRunFileReader.reset();
        joinInMemoryPartitions(leftRunFileReader, writer);
        leftRunFileReader.close();
    }

    private void joinInMemoryPartitions(RunFileReaderDir runFileReader, IFrameWriter writer)
            throws HyracksDataException {
        int leftPid = 0;
        // Prepare frame.
        if (runFileReader.nextFrame(tmpFrame)) {
            joinTupleAccessor.reset(tmpFrame.getBuffer());
            joinTupleAccessor.next();
            spillJoinReadCount++;
        }
        while (joinTupleAccessor.exists()) {
            joinInMemoryPartitionTuple(leftPid, writer);
            leftPid = loadNextTuple(joinTupleAccessor, leftPid, runFileReader, tmpFrame);
        }
    }

    private int loadNextTuple(ITupleAccessor accessor, int leftPid, RunFileReaderDir reader, IFrame frame)
            throws HyracksDataException {
        accessor.next();
        if (!accessor.exists()) {
            // Load next frame.
            long start = System.nanoTime();
            boolean nextFrame = reader.nextFrame(frame);
            ioCumulativeTime += System.nanoTime() - start;
            if (nextFrame) {
                accessor.reset(frame.getBuffer());
                accessor.next();
                spillJoinReadCount++;
                if (reader.getReadPointer() == leftPartitionOffsets.get(leftPid + 1)) {
                    resetInMemoryPartitions();
                    leftPid = leftPid + 1;
                }
            }
        }
        return leftPid;
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

    private void processSpill(DisjointIntervalPartitionAndSpill dipas, LinkedList<Long> rpc, RunFileWriterDir rfw, LinkedList<Long> offsets) throws HyracksDataException {
        while (dipas.getSpillSizeInTup() > 0) {
            RunFileReaderDir rfr = dipas.getSpillRFReader();
            dipas.reset(getNewSpillWriter());
            rfr.open();
            while (rfr.nextFrame(tmpFrame)) {
                dipas.processFrame(tmpFrame.getBuffer());
                spillPartitionReadCount++;
            }
            rfr.close();
            getRunFileReaders(dipas, rpc, rfw, offsets);
        }
    }

    private void getRunFileReaders(DisjointIntervalPartitionAndSpill dipas, LinkedList<Long> rpc, RunFileWriterDir rfw, LinkedList<Long> offsets) throws HyracksDataException {
        //        int offset = rfrs.size();
        dipas.spillAllPartitions();
        spillPartitionWriteCount += dipas.getSpillWriteCount();
        for (int i = 0; i < numberOfPartitions; i++) {
            if (dipas.getPartitionSizeInTup(i) > 0) {
                RunFileReaderDir reader = dipas.getRFReader(i);
                rpc.add(dipas.getPartitionSizeInTup(i));
                //                printRunFileTuples("spilled " + (i + offset) + " on " + partition, dipas.getRFReader(i));
                appendToSingleRunFile(reader, rfw, offsets);
                reader.close();
                reader.delete();
            } else {
                break;
            }
        }
    }

    private void appendToSingleRunFile(RunFileReaderDir reader, RunFileWriterDir rfw, LinkedList<Long> offsets)
            throws HyracksDataException {
        reader.open();
        while (reader.nextFrame(tmpFrame)) {
            rfw.nextFrame(tmpFrame.getBuffer());
        }
        offsets.add(rfw.getFileSize());
        reader.close();
    }

    private void printRunFileTuples(String message, RunFileReaderDir rfReader) throws HyracksDataException {
        rfReader.open();
        rfReader.reset();
        if (rfReader.nextFrame(tmpFrame)) {
            joinTupleAccessor.reset(tmpFrame.getBuffer());
            joinTupleAccessor.next();
        }
        while (joinTupleAccessor.exists()) {
            int pid = 0;
            //            TuplePrinterUtil.printTuple("RunFile: " + message, joinTupleAccessor);
            loadNextTuple(joinTupleAccessor, pid, rfReader, tmpFrame);
        }
        rfReader.close();
    }

    private void processSpilledJoin(IFrameWriter writer) throws HyracksDataException {
        VPartitionTupleBufferManager buffer =
                new VPartitionTupleBufferManager(ctx, VPartitionTupleBufferManager.NO_CONSTRAIN, numberOfPartitions,
                        numberOfPartitions * ctx.getInitialFrameSize());
        TuplePointer tpTemp = new TuplePointer();
        int partitionId = 0;
//        int countLoads = 0;
        TupleAccessor tupleAccessor = new TupleAccessor(rightRd);

        rightRunFileReader.open();
        rightRunFileReader.reset();
        // Prepare frame.
       while (rightRunFileReader.nextFrame(tmpFrame2)) {
            tupleAccessor.reset(tmpFrame2.getBuffer());
            spillJoinReadCount++;
            if (rightRunFileReader.getReadPointer() == rightPartitionOffsets.get(partitionId + 1)) {
                // Next partition
                partitionId = partitionId + 1;
            }
            tupleAccessor.next();
            // Loop over all tuples
            while (tupleAccessor.exists()) {
                boolean inserted = (partitionId >= numberOfPartitions
                        || !buffer.insertTuple(partitionId, tupleAccessor, tupleAccessor.getTupleId(), tpTemp));
                if (inserted) {
//                    statsStream += (java.time.LocalDateTime.now() + " --- <" + partition + "> Batch: " + countLoads++
//                            + " Load Partitions: " + partitionId + "\n");
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
        }
        rightRunFileReader.close();
//        statsStream += (java.time.LocalDateTime.now() + " --- <" + partition + "> Batch: " + countLoads++
//                + " Load Partitions: " + partitionId + "\n");
        getInMemoryTupleAccessors(buffer);
        processInMemoryJoin(writer);
//        statsStream +=
//                (java.time.LocalDateTime.now() + " --- <" + partition + "> Batch: " + countLoads + " DONE" + "\n");
//        System.out.println(statsStream);
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
        leftRunFileReader.close();
        leftRunFileReader.delete();
        rightRunFileReader.close();
        rightRunFileReader.delete();
    }

}

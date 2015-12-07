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
package org.apache.asterix.runtime.operators.joins;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.logging.Logger;

import org.apache.asterix.dataflow.data.nontagged.serde.AIntervalSerializerDeserializer;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.IActivity;
import org.apache.hyracks.api.dataflow.IActivityGraphBuilder;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.TaskId;
import org.apache.hyracks.api.dataflow.value.INullWriter;
import org.apache.hyracks.api.dataflow.value.IPredicateEvaluator;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.ITuplePairComparator;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.comm.io.FrameTuplePairComparator;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFamily;
import org.apache.hyracks.dataflow.common.data.partition.range.IRangeMap;
import org.apache.hyracks.dataflow.common.io.RunFileReader;
import org.apache.hyracks.dataflow.common.io.RunFileWriter;
import org.apache.hyracks.dataflow.std.base.AbstractActivityNode;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractStateObject;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import org.apache.hyracks.dataflow.std.join.IMergeJoinCheckerFactory;
import org.apache.hyracks.dataflow.std.join.InMemoryHashJoin;
import org.apache.hyracks.dataflow.std.join.NestedLoopJoin;
import org.apache.hyracks.dataflow.std.join.OptimizedHybridHashJoin;
import org.apache.hyracks.dataflow.std.structures.ISerializableTable;
import org.apache.hyracks.dataflow.std.structures.SerializableHashTable;

public class IntervalPartitionJoinOperatorDescriptor extends AbstractOperatorDescriptor {
    private static final int BUILD_AND_PARTITION_ACTIVITY_ID = 0;
    private static final int PARTITION_AND_JOIN_ACTIVITY_ID = 1;

    private final int memsize;
    private static final long serialVersionUID = 1L;
    private final int inputsize0;
    private final double factor;
    private final int recordsPerFrame;
    private final int keys0;
    private final int keys1;
    private final IMergeJoinCheckerFactory mjcf;
    private final IRangeMap rangeMap;

    private final IPredicateEvaluator predEvaluator;
    private boolean isReversed; //Added for handling correct calling for predicate-evaluator upon recursive calls that cause role-reversal

    private static final Logger LOGGER = Logger.getLogger(IntervalPartitionJoinOperatorDescriptor.class.getName());

    public IntervalPartitionJoinOperatorDescriptor(IOperatorDescriptorRegistry spec, int memsize, int inputsize0,
            int recordsPerFrame, double factor, int[] keys0, int[] keys1, RecordDescriptor recordDescriptor,
            IMergeJoinCheckerFactory mjcf, IRangeMap rangeMap) {
        super(spec, 2, 1);
        this.memsize = memsize;
        this.inputsize0 = inputsize0;
        this.factor = factor;
        this.recordsPerFrame = recordsPerFrame;
        this.keys0 = keys0[0];
        this.keys1 = keys1[0];
        recordDescriptors[0] = recordDescriptor;
        this.mjcf = mjcf;
        this.rangeMap = rangeMap;
    }

    @Override
    public void contributeActivities(IActivityGraphBuilder builder) {
        ActivityId p1Aid = new ActivityId(odId, BUILD_AND_PARTITION_ACTIVITY_ID);
        ActivityId p2Aid = new ActivityId(odId, PARTITION_AND_JOIN_ACTIVITY_ID);
        IActivity phase1 = new BuildAndPartitionActivityNode(p1Aid, p2Aid);
        IActivity phase2 = new PartitionAndJoinActivityNode(p2Aid, p1Aid);

        builder.addActivity(this, phase1);
        builder.addSourceEdge(1, phase1, 0);

        builder.addActivity(this, phase2);
        builder.addSourceEdge(0, phase2, 0);

        builder.addBlockingEdge(phase1, phase2);

        builder.addTargetEdge(0, phase2, 0);
    }

    public static class BuildAndPartitionTaskState extends AbstractStateObject {
        private RunFileWriter[] fWriters;
        private InMemoryIntervalPartitionJoin joiner;
        private IntervalPartitionSpillablePartitions ipsp;
        private int nPartitions;
        private int memoryForHashtable;

        public BuildAndPartitionTaskState() {
        }

        private BuildAndPartitionTaskState(JobId jobId, TaskId taskId) {
            super(jobId, taskId);
        }

        @Override
        public void toBytes(DataOutput out) throws IOException {

        }

        @Override
        public void fromBytes(DataInput in) throws IOException {

        }

    }

    public class IntervalPartition {
        private int partitionI;
        private int partitionJ;

        public IntervalPartition(int i, int j) {
            reset(i, j);
        }

        public void reset(int i, int j) {
            partitionI = i;
            partitionJ = j;
        }

        public int getI() {
            return partitionI;
        }

        public int getJ() {
            return partitionJ;
        }

        public int hashCode() {
            return (int) partitionI << 4 & (int) partitionJ;
        }

        public int partition(int k) {
            long duration = partitionJ - partitionI;
            int p;
            for (p = 0; p < duration - 1; ++p) {
                p += k - duration + 1;
            }
            p += partitionI;
            return p;
        }

    }

    private long getStartPartition(int partition) throws HyracksDataException {
        int fieldIndex = 0;
        if (ATypeTag.INT64.serialize() != rangeMap.getTag(0, 0)) {
            throw new HyracksDataException("Invalid range map type for interval merge join checker.");
        }
        long partitionStart = Long.MIN_VALUE;
        if (partition != 0 && partition <= rangeMap.getSplitCount()) {
            partitionStart = LongPointable.getLong(rangeMap.getByteArray(fieldIndex, partition - 1),
                    rangeMap.getStartOffset(fieldIndex, partition - 1) + 1);
        } else if (partition > rangeMap.getSplitCount()) {
            partitionStart = Long.MAX_VALUE;
        }
        return partitionStart;
    }

    private long getEndPartition(int partition) throws HyracksDataException {
        int fieldIndex = 0;
        if (ATypeTag.INT64.serialize() != rangeMap.getTag(0, 0)) {
            throw new HyracksDataException("Invalid range map type for interval merge join checker.");
        }
        long partitionEnd = Long.MAX_VALUE;
        if (partition < rangeMap.getSplitCount()) {
            partitionEnd = LongPointable.getLong(rangeMap.getByteArray(fieldIndex, partition),
                    rangeMap.getStartOffset(fieldIndex, partition) + 1);
        }
        return partitionEnd;
    }

    private int determineK() {
        return 10;
    }

    protected long getIntervalStart(IFrameTupleAccessor accessor, int tupleId, int fieldId)
            throws HyracksDataException {
        int start = accessor.getTupleStartOffset(tupleId) + accessor.getFieldSlotsLength()
                + accessor.getFieldStartOffset(tupleId, fieldId) + 1;
        return AIntervalSerializerDeserializer.getIntervalStart(accessor.getBuffer().array(), start);
    }

    protected long getIntervalEnd(IFrameTupleAccessor accessor, int tupleId, int fieldId) throws HyracksDataException {
        int start = accessor.getTupleStartOffset(tupleId) + accessor.getFieldSlotsLength()
                + accessor.getFieldStartOffset(tupleId, fieldId) + 1;
        return AIntervalSerializerDeserializer.getIntervalEnd(accessor.getBuffer().array(), start);
    }

    private IntervalPartition getIntervalPartition(IFrameTupleAccessor accessorBuild, int tIndex, int fieldId,
            long partitionStart, long partitionDuration) throws HyracksDataException {
        return new IntervalPartition(
                getIntervalPartitionI(accessorBuild, tIndex, fieldId, partitionStart, partitionDuration),
                getIntervalPartitionJ(accessorBuild, tIndex, fieldId, partitionStart, partitionDuration));
    }

    private int getIntervalPartitionI(IFrameTupleAccessor accessorBuild, int tIndex, int fieldId, long partitionStart,
            long partitionDuration) throws HyracksDataException {
        return (int) Math.floorDiv((getIntervalStart(accessorBuild, tIndex, fieldId) - partitionStart),
                partitionDuration);
    }

    private int getIntervalPartitionJ(IFrameTupleAccessor accessorBuild, int tIndex, int fieldId, long partitionStart,
            long partitionDuration) throws HyracksDataException {
        return (int) Math.floorDiv((getIntervalEnd(accessorBuild, tIndex, fieldId) - partitionStart),
                partitionDuration);
    }

    private class BuildAndPartitionActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        private final ActivityId joinAid;

        public BuildAndPartitionActivityNode(ActivityId id, ActivityId joinAid) {
            super(id);
            this.joinAid = joinAid;
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, final int partition, final int nPartitions)
                        throws HyracksDataException {
            final RecordDescriptor probeRd = recordDescProvider.getInputRecordDescriptor(joinAid, 0);
            final RecordDescriptor buildRd = recordDescProvider.getInputRecordDescriptor(getActivityId(), 0);
            final long partitionStart = getStartPartition(partition);
            final long partitionEnd = getEndPartition(partition);
            final long partitionDuration = partitionEnd - partitionStart;
            final int k = determineK();

            IOperatorNodePushable op = new AbstractUnaryInputSinkOperatorNodePushable() {
                private BuildAndPartitionTaskState state = new BuildAndPartitionTaskState(
                        ctx.getJobletContext().getJobId(), new TaskId(getActivityId(), partition));
                ITuplePartitionComputer probeHpc = new IntervalPartitionComputerFactory(keys0, k, partitionStart,
                        partitionDuration).createPartitioner();
                ITuplePartitionComputer buildHpc = new IntervalPartitionComputerFactory(keys1, k, partitionStart,
                        partitionDuration).createPartitioner();

                @Override
                public void close() throws HyracksDataException {
                    state.ipsp.closeBuild();
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    state.ipsp.build(buffer);
                }

                @Override
                public void open() throws HyracksDataException {
                    if (memsize <= 2) {
                        // Dedicated buffers: One buffer to read and one buffer for output
                        throw new HyracksDataException("not enough memory for interval join");
                    }
                    state.ipsp = new IntervalPartitionSpillablePartitions(ctx, memsize, nPartitions, "ipj", buildRd,
                            buildHpc, probeRd, probeHpc);
                    state.ipsp.initBuild();
                    LOGGER.fine("IntervalPartitionJoin is starting the build phase with " + nPartitions
                            + " partitions using " + memsize + " frames for memory.");
                }

                @Override
                public void fail() throws HyracksDataException {
                }
            };
            return op;
        }
    }

    private class PartitionAndJoinActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        private final ActivityId buildAid;

        public PartitionAndJoinActivityNode(ActivityId id, ActivityId buildAid) {
            super(id);
            this.buildAid = buildAid;
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, final int partition, final int nPartitions)
                        throws HyracksDataException {
            final long partitionStart = getStartPartition(partition);
            final long partitionEnd = getEndPartition(partition);
            final long partitionDuration = partitionEnd - partitionStart;
            final int k = determineK();

            final RecordDescriptor probeRd = recordDescProvider.getInputRecordDescriptor(getActivityId(), 0);
            final RecordDescriptor buildRd = recordDescProvider.getInputRecordDescriptor(buildAid, 0);

            IOperatorNodePushable op = new AbstractUnaryInputUnaryOutputOperatorNodePushable() {
                private BuildAndPartitionTaskState state;
                private final FrameTupleAccessor accessorProbe = new FrameTupleAccessor(probeRd);
                ITuplePartitionComputer probeHpc = new IntervalPartitionComputerFactory(keys0, k, partitionStart,
                        partitionDuration).createPartitioner();
                ITuplePartitionComputer buildHpc = new IntervalPartitionComputerFactory(keys1, k, partitionStart,
                        partitionDuration).createPartitioner();

                private final FrameTupleAppender appender = new FrameTupleAppender();
                private final FrameTupleAppender ftap = new FrameTupleAppender();
                private final IFrame inBuffer = new VSizeFrame(ctx);
                private final IFrame outBuffer = new VSizeFrame(ctx);

                @Override
                public void open() throws HyracksDataException {
                    state.ipsp.initProbe();
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    if (state.ipsp.hasSpilledParts()) {
                        // TODO make probe spill by tuple
                        state.ipsp.probe(buffer, writer);
                    }

                    accessorProbe.reset(buffer);
                    int tupleCount = accessorProbe.getTupleCount();

                    boolean print = false;
                    if (print) {
                        accessorProbe.prettyPrint();
                    }

                    for (int i = 0; i < tupleCount; ++i) {
                        int pid = probeHpc.partition(accessorProbe, i, state.ipsp.getNumberOfPartitions());

                        if (state.ipsp.getBuildPartitionSizeInTup(pid) > 0) {
                            // Tuple has potential match from previous phase
                            if (state.ipsp.hasSpilledPartition(pid)) {
                                //pid is Spilled
                            } else {
                                //pid is Resident
                                nestedLoopJoin( accessorProbe,  i,  pid,  writer);
                            }
                        }
                    }
                }

                public void nestedLoopJoin(IFrameTupleAccessor accessorProbe, int tid, int pid, IFrameWriter writer) throws HyracksDataException {
                    state.ipsp.memoryIteratorReset(pid);
                    IFrameTupleAccessor accessor;
                    while (state.ipsp.memoryIteratorHasNext()) {
                        accessor.reset(state.ipsp.memoryIteratorNext());
                        int tupleCount = accessor.getTupleCount();
                        for (int i = 0; i < tupleCount; ++i) {
                            if (evaluatePredicate(accessor, i,accessorProbe,  tid)) {
                                appendToResult(accessor, i,accessorProbe,  tid, writer);
                            }
                        }
                    }
                    }

                private boolean evaluatePredicate(IFrameTupleAccessor accessorBuild, int tBuildId, IFrameTupleAccessor accessorProbe, int tProbeId) {
                    if (isReversed) {
                        // Role Reversal Optimization is triggered
                        return ((predEvaluator == null) || predEvaluator.evaluate(accessorBuild, tProbeId, accessorProbe, tBuildId));
                    } else {
                        return ((predEvaluator == null) || predEvaluator.evaluate(accessorProbe, tBuildId, accessorBuild, tProbeId));
                    }
                }

                private void appendToResult(IFrameTupleAccessor accessorBuild, int tBuildId, IFrameTupleAccessor accessorProbe, int tProbeId, IFrameWriter writer) throws HyracksDataException {
                    if (!isReversed) {
                        FrameUtils.appendConcatToWriter(writer, appender, accessorProbe, tProbeId, accessorBuild,
                                tBuildId);
                    } else {
                        FrameUtils.appendConcatToWriter(writer, appender, accessorBuild, tBuildId, accessorProbe,
                                tProbeId);
                    }
                }


                @Override
                public void fail() throws HyracksDataException {
                    writer.fail();
                }

                @Override
                public void close() throws HyracksDataException {
                    state.ipsp.closeProbe(writer);

                    BitSet partitionStatus = state.ipsp.getPartitionStatus();

                    rPartbuff.reset();
                    for (int pid = partitionStatus.nextSetBit(0); pid >= 0; pid = partitionStatus.nextSetBit(pid + 1)) {

                        RunFileReader bReader = state.ipsp.getBuildRFReader(pid);
                        RunFileReader pReader = state.ipsp.getProbeRFReader(pid);

                        if (bReader == null || pReader == null) { //either of sides (or both) does not have any tuple, thus no need for joining (no potential match)
                            continue;
                        }
                        int bSize = state.ipsp.getBuildPartitionSizeInTup(pid);
                        int pSize = state.ipsp.getProbePartitionSizeInTup(pid);
                        int beforeMax = (bSize > pSize) ? bSize : pSize;
                        joinPartitionPair(state.ipsp, bReader, pReader, pid, beforeMax, 1, false);
                    }
                    writer.close();
                    LOGGER.fine("OptimizedHybridHashJoin closed its probe phase");
                }

                private void joinPartitionPair(IntervalPartitionSpillablePartitions ipsp, RunFileReader buildSideReader,
                        RunFileReader probeSideReader, int pid, int beforeMax, int level, boolean wasReversed)
                                throws HyracksDataException {
                    ITuplePartitionComputer probeHpc = new FieldHashPartitionComputerFamily(probeKeys,
                            hashFunctionGeneratorFactories).createPartitioner(level);
                    ITuplePartitionComputer buildHpc = new FieldHashPartitionComputerFamily(buildKeys,
                            hashFunctionGeneratorFactories).createPartitioner(level);

                    long buildPartSize = wasReversed ? (ipsp.getProbePartitionSize(pid) / ctx.getInitialFrameSize())
                            : (ipsp.getBuildPartitionSize(pid) / ctx.getInitialFrameSize());
                    long probePartSize = wasReversed ? (ipsp.getBuildPartitionSize(pid) / ctx.getInitialFrameSize())
                            : (ipsp.getProbePartitionSize(pid) / ctx.getInitialFrameSize());

                    LOGGER.fine("\n>>>Joining Partition Pairs (thread_id " + Thread.currentThread().getId() + ") (pid "
                            + pid + ") - (level " + level + ") - wasReversed " + wasReversed + " - BuildSize:\t"
                            + buildPartSize + "\tProbeSize:\t" + probePartSize + " - MemForJoin " + (state.memForJoin)
                            + "  - LeftOuter is " + isLeftOuter);

                    //Apply (Recursive) HHJ
                    else {
                        LOGGER.fine("\t>>>Case 2. ApplyRecursiveHHJ - [Level " + level + "]");
                        OptimizedHybridHashJoin rHHj;
                        if (!forceRR && (isLeftOuter || buildPartSize < probePartSize)) { //Case 2.1 - Recursive HHJ (wout Role-Reversal)
                            LOGGER.fine("\t\t>>>Case 2.1 - RecursiveHHJ WITH (isLeftOuter || build<probe) - [Level "
                                    + level + "]");
                            int n = getNumberOfPartitions(state.memForJoin, (int) buildPartSize, fudgeFactor,
                                    nPartitions);
                            rHHj = new OptimizedHybridHashJoin(ctx, state.memForJoin, n, PROBE_REL, BUILD_REL,
                                    probeKeys, buildKeys, comparators, probeRd, buildRd, probeHpc, buildHpc,
                                    predEvaluator); //checked-confirmed

                            buildSideReader.open();
                            rHHj.initBuild();
                            rPartbuff.reset();
                            while (buildSideReader.nextFrame(rPartbuff)) {
                                rHHj.build(rPartbuff.getBuffer());
                            }

                            rHHj.closeBuild();

                            probeSideReader.open();
                            rHHj.initProbe();
                            rPartbuff.reset();
                            while (probeSideReader.nextFrame(rPartbuff)) {
                                rHHj.probe(rPartbuff.getBuffer(), writer);
                            }
                            rHHj.closeProbe(writer);

                            int maxAfterBuildSize = rHHj.getMaxBuildPartitionSize();
                            int maxAfterProbeSize = rHHj.getMaxProbePartitionSize();
                            int afterMax = (maxAfterBuildSize > maxAfterProbeSize) ? maxAfterBuildSize
                                    : maxAfterProbeSize;

                            BitSet rPStatus = rHHj.getPartitionStatus();
                            if (!forceNLJ && (afterMax < (NLJ_SWITCH_THRESHOLD * beforeMax))) { //Case 2.1.1 - Keep applying HHJ
                                LOGGER.fine(
                                        "\t\t>>>Case 2.1.1 - KEEP APPLYING RecursiveHHJ WITH (isLeftOuter || build<probe) - [Level "
                                                + level + "]");
                                for (int rPid = rPStatus.nextSetBit(0); rPid >= 0; rPid = rPStatus
                                        .nextSetBit(rPid + 1)) {
                                    RunFileReader rbrfw = rHHj.getBuildRFReader(rPid);
                                    RunFileReader rprfw = rHHj.getProbeRFReader(rPid);

                                    if (rbrfw == null || rprfw == null) {
                                        continue;
                                    }

                                    joinPartitionPair(rHHj, rbrfw, rprfw, rPid, afterMax, (level + 1), false); //checked-confirmed
                                }

                            } else { //Case 2.1.2 - Switch to NLJ
                                LOGGER.fine(
                                        "\t\t>>>Case 2.1.2 - SWITCHED to NLJ RecursiveHHJ WITH (isLeftOuter || build<probe) - [Level "
                                                + level + "]");
                                for (int rPid = rPStatus.nextSetBit(0); rPid >= 0; rPid = rPStatus
                                        .nextSetBit(rPid + 1)) {
                                    RunFileReader rbrfw = rHHj.getBuildRFReader(rPid);
                                    RunFileReader rprfw = rHHj.getProbeRFReader(rPid);

                                    if (rbrfw == null || rprfw == null) {
                                        continue;
                                    }

                                    int buildSideInTups = rHHj.getBuildPartitionSizeInTup(rPid);
                                    int probeSideInTups = rHHj.getProbePartitionSizeInTup(rPid);
                                    if (isLeftOuter || buildSideInTups < probeSideInTups) {
                                        applyNestedLoopJoinOnFiles(buildRd, probeRd, memsize, rprfw, rbrfw, nljComparator0,
                                                false); //checked-modified
                                    } else {
                                        applyNestedLoopJoinOnFiles(probeRd, buildRd, memsize, rbrfw, rprfw, nljComparator1,
                                                true); //checked-modified
                                    }
                                }
                            }
                        } else { //Case 2.2 - Recursive HHJ (with Role-Reversal)
                            LOGGER.fine("\t\t>>>Case 2.2. - RecursiveHHJ WITH RoleReversal - [Level " + level + "]");
                            int n = getNumberOfPartitions(state.memForJoin, (int) probePartSize, fudgeFactor,
                                    nPartitions);

                            rHHj = new OptimizedHybridHashJoin(ctx, state.memForJoin, n, BUILD_REL, PROBE_REL,
                                    buildKeys, probeKeys, comparators, buildRd, probeRd, buildHpc, probeHpc,
                                    predEvaluator); //checked-confirmed
                            rHHj.setIsReversed(true); //Added to use predicateEvaluator (for inMemoryHashJoin) correctly

                            probeSideReader.open();
                            rHHj.initBuild();
                            rPartbuff.reset();
                            while (probeSideReader.nextFrame(rPartbuff)) {
                                rHHj.build(rPartbuff.getBuffer());
                            }
                            rHHj.closeBuild();
                            rHHj.initProbe();
                            buildSideReader.open();
                            rPartbuff.reset();
                            while (buildSideReader.nextFrame(rPartbuff)) {
                                rHHj.probe(rPartbuff.getBuffer(), writer);
                            }
                            rHHj.closeProbe(writer);
                            int maxAfterBuildSize = rHHj.getMaxBuildPartitionSize();
                            int maxAfterProbeSize = rHHj.getMaxProbePartitionSize();
                            int afterMax = (maxAfterBuildSize > maxAfterProbeSize) ? maxAfterBuildSize
                                    : maxAfterProbeSize;
                            BitSet rPStatus = rHHj.getPartitionStatus();

                            } else { //Case 2.2.2 - Switch to NLJ
                                LOGGER.fine(
                                        "\t\t>>>Case 2.2.2 - SWITCHED to NLJ RecursiveHHJ WITH RoleReversal - [Level "
                                                + level + "]");
                                for (int rPid = rPStatus.nextSetBit(0); rPid >= 0; rPid = rPStatus
                                        .nextSetBit(rPid + 1)) {
                                    RunFileReader rbrfw = rHHj.getBuildRFReader(rPid);
                                    RunFileReader rprfw = rHHj.getProbeRFReader(rPid);

                                    if (rbrfw == null || rprfw == null) {
                                        continue;
                                    }

                                    long buildSideSize = rbrfw.getFileSize();
                                    long probeSideSize = rprfw.getFileSize();
                                    if (buildSideSize > probeSideSize) {
                                        applyNestedLoopJoinOnFiles(buildRd, probeRd, memsize, rbrfw, rprfw, nljComparator0,
                                                true); //checked-modified
                                    } else {
                                        applyNestedLoopJoinOnFiles(probeRd, buildRd, memsize, rprfw, rbrfw, nljComparator1,
                                                true); //checked-modified
                                    }
                                }
                            }
                        }
                        buildSideReader.close();
                        probeSideReader.close();
                    }
                }

        private void applyNestedLoopJoinOnFiles(RecordDescriptor outerRd, RecordDescriptor innerRd, int memorySize,
                RunFileReader outerReader, RunFileReader innerReader, ITuplePairComparator nljComparator,
                boolean reverse) throws HyracksDataException {
            NestedLoopJoin nlj = new NestedLoopJoin(ctx, new FrameTupleAccessor(outerRd),
                    new FrameTupleAccessor(innerRd), nljComparator, memorySize, predEvaluator, false, null);
            nlj.setIsReversed(reverse);

            IFrame cacheBuff = new VSizeFrame(ctx);
            innerReader.open();
            while (innerReader.nextFrame(cacheBuff)) {
                nlj.cache(cacheBuff.getBuffer());
                cacheBuff.reset();
            }
            nlj.closeCache();

            IFrame joinBuff = new VSizeFrame(ctx);
            outerReader.open();

            while (outerReader.nextFrame(joinBuff)) {
                nlj.join(joinBuff.getBuffer(), writer);
                joinBuff.reset();
            }

            nlj.closeJoin(writer);
            outerReader.close();
            innerReader.close();
        }
    };return op;
}}

}
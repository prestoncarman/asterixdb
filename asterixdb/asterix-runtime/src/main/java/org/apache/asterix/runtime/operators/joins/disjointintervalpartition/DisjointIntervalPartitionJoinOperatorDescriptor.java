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
import java.util.PriorityQueue;
import java.util.logging.Logger;

import org.apache.asterix.runtime.operators.joins.IIntervalMergeJoinChecker;
import org.apache.asterix.runtime.operators.joins.IIntervalMergeJoinCheckerFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.ActivityId;
import org.apache.hyracks.api.dataflow.IActivity;
import org.apache.hyracks.api.dataflow.IActivityGraphBuilder;
import org.apache.hyracks.api.dataflow.IOperatorNodePushable;
import org.apache.hyracks.api.dataflow.TaskId;
import org.apache.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.IOperatorDescriptorRegistry;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.dataflow.common.comm.util.FrameUtils;
import org.apache.hyracks.dataflow.std.base.AbstractActivityNode;
import org.apache.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import org.apache.hyracks.dataflow.std.base.AbstractStateObject;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import org.apache.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;

public class DisjointIntervalPartitionJoinOperatorDescriptor extends AbstractOperatorDescriptor {
    private static final long serialVersionUID = 1L;

    private static final int JOIN_BUILD_ACTIVITY_ID = 0;
    private static final int JOIN_PROBE_ACTIVITY_ID = 1;
    private final int[] leftKeys;
    private final int[] rightKeys;
    private final int memoryForJoin;
    private final IIntervalMergeJoinCheckerFactory imjcf;

    private final int probeKey;
    private final int buildKey;

    private static final Logger LOGGER =
            Logger.getLogger(DisjointIntervalPartitionJoinOperatorDescriptor.class.getName());

    public DisjointIntervalPartitionJoinOperatorDescriptor(IOperatorDescriptorRegistry spec, int memoryForJoin,
            int[] leftKeys, int[] rightKeys, RecordDescriptor recordDescriptor,
            IIntervalMergeJoinCheckerFactory imjcf) {
        super(spec, 2, 1);
        recordDescriptors[0] = recordDescriptor;
        this.buildKey = leftKeys[0];
        this.probeKey = rightKeys[0];
        this.leftKeys = leftKeys;
        this.rightKeys = rightKeys;
        this.memoryForJoin = memoryForJoin;
        this.imjcf = imjcf;
    }

    @Override
    public void contributeActivities(IActivityGraphBuilder builder) {
        ActivityId leftAid = new ActivityId(odId, JOIN_BUILD_ACTIVITY_ID);
        ActivityId rightAid = new ActivityId(odId, JOIN_PROBE_ACTIVITY_ID);

        IActivity leftAN = new JoinProbeActivityNode(rightAid);
        IActivity rightAN = new JoinBuildActivityNode(leftAid, rightAid);

        builder.addActivity(this, rightAN);
        builder.addSourceEdge(1, rightAN, 0);

        builder.addActivity(this, leftAN);
        builder.addSourceEdge(0, leftAN, 0);
        builder.addTargetEdge(0, leftAN, 0);
        builder.addBlockingEdge(rightAN, leftAN);
    }

    public static class JoinCacheTaskState extends AbstractStateObject {
        private DisjointIntervalPartitionJoiner joiner;

        private JoinCacheTaskState(JobId jobId, TaskId taskId) {
            super(jobId, taskId);
        }
    }

    private class JoinBuildActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        private final ActivityId nljAid;

        public JoinBuildActivityNode(ActivityId id, ActivityId nljAid) {
            super(id);
            this.nljAid = nljAid;
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions) {
            final RecordDescriptor rd0 = recordDescProvider.getInputRecordDescriptor(nljAid, 0);
            final RecordDescriptor rd1 = recordDescProvider.getInputRecordDescriptor(getActivityId(), 0);

            return new AbstractUnaryInputSinkOperatorNodePushable() {
                private JoinCacheTaskState state;

                @Override
                public void open() throws HyracksDataException {
                    state = new JoinCacheTaskState(ctx.getJobletContext().getJobId(),
                            new TaskId(getActivityId(), partition));

                    PriorityQueue<PartitionMinItem> partitionMinRight = new PriorityQueue<>(16);
                    PriorityQueue<PartitionMinItem> partitionMinLeft = new PriorityQueue<>(16);

                    DisjointIntervalPartitionComputer leftDipc =
                            new DisjointIntervalPartitionComputerFactory(buildKey, partitionMinRight)
                                    .createPartitioner();
                    DisjointIntervalPartitionComputer rightDipc =
                            new DisjointIntervalPartitionComputerFactory(probeKey, partitionMinLeft)
                                    .createPartitioner();
                    IIntervalMergeJoinChecker imjc = imjcf.createMergeJoinChecker(leftKeys, rightKeys, ctx);

                    state.joiner = new DisjointIntervalPartitionJoiner(ctx, memoryForJoin, partition, imjc, buildKey,
                            probeKey, rd0, rd1, leftDipc, rightDipc);

                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    ByteBuffer copyBuffer = ctx.allocateFrame(buffer.capacity());
                    FrameUtils.copyAndFlip(buffer, copyBuffer);
                    state.joiner.processBuildFrame(copyBuffer);
                }

                @Override
                public void close() throws HyracksDataException {
                    state.joiner.processBuildClose();
                    ctx.setStateObject(state);
                }

                @Override
                public void fail() throws HyracksDataException {
                    // No variables to update.
                }
            };
        }
    }

    private class JoinProbeActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        public JoinProbeActivityNode(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions) {
            return new AbstractUnaryInputUnaryOutputOperatorNodePushable() {
                private JoinCacheTaskState state;

                @Override
                public void open() throws HyracksDataException {
                    writer.open();
                    state = (JoinCacheTaskState) ctx.getStateObject(
                            new TaskId(new ActivityId(getOperatorId(), JOIN_BUILD_ACTIVITY_ID), partition));
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    state.joiner.processProbeFrame(buffer, writer);
                }

                @Override
                public void close() throws HyracksDataException {
                    try {
                        state.joiner.processProbeClose(writer);
                    } finally {
                        writer.close();
                    }
                }

                @Override
                public void fail() throws HyracksDataException {
                    writer.fail();
                }
            };
        }
    }
}
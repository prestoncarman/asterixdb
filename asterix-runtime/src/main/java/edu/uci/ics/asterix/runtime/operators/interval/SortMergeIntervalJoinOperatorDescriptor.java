/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.runtime.operators.interval;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.ActivityId;
import edu.uci.ics.hyracks.api.dataflow.IActivityGraphBuilder;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.TaskId;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.IOperatorDescriptorRegistry;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractActivityNode;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractStateObject;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryOutputOperatorNodePushable;

public class SortMergeIntervalJoinOperatorDescriptor extends AbstractOperatorDescriptor {
    private static final int LEFT_ACTIVITY_ID = 0;
    private static final int RIGHT_ACTIVITY_ID = 1;

    public SortMergeIntervalJoinOperatorDescriptor(IOperatorDescriptorRegistry spec, int nInputs,
            RecordDescriptor recordDescriptor) {
        super(spec, nInputs, 1);
        recordDescriptors[0] = recordDescriptor;
    }

    private static final long serialVersionUID = 1L;

    @Override
    public void contributeActivities(IActivityGraphBuilder builder) {
        SortMergeIntervalJoinLocks locks = new SortMergeIntervalJoinLocks();
        ActivityId p1Aid = new ActivityId(odId, LEFT_ACTIVITY_ID);
        ActivityId p2Aid = new ActivityId(odId, RIGHT_ACTIVITY_ID);
        LeftActivityNode phase1 = new LeftActivityNode(p1Aid, p2Aid, locks);
        RightActivityNode phase2 = new RightActivityNode(p2Aid, p1Aid, locks);

        builder.addActivity(this, phase1);
        builder.addSourceEdge(1, phase1, 0);

        builder.addActivity(this, phase2);
        builder.addSourceEdge(0, phase2, 0);

        builder.addTargetEdge(0, phase2, 0);
    }

    public static class SortMergeIntervalJoinTaskState extends AbstractStateObject {
        private SortMergeIntervalStatus status;
        private SortMergeIntervalJoiner joiner;
        private ByteBuffer left;
        private ByteBuffer right;
        private boolean failed;

        public SortMergeIntervalJoinTaskState() {
        }

        private SortMergeIntervalJoinTaskState(JobId jobId, TaskId taskId) {
            super(jobId, taskId);
        }

        @Override
        public void toBytes(DataOutput out) throws IOException {

        }

        @Override
        public void fromBytes(DataInput in) throws IOException {
        }
    }

    private class LeftActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        private final ActivityId joinAid;

        private final SortMergeIntervalJoinLocks locks;

        public LeftActivityNode(ActivityId id, ActivityId joinAid, SortMergeIntervalJoinLocks locks) {
            super(id);
            this.joinAid = joinAid;
            this.locks = locks;
        }

        @Override
        public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions)
                throws HyracksDataException {
            locks.setPartitions(nPartitions);
            RecordDescriptor inRecordDesc = recordDescProvider.getInputRecordDescriptor(getActivityId(), 0);
            return new LeftOperator(ctx, partition, inRecordDesc, locks);
        }

        private class LeftOperator extends AbstractUnaryOutputOperatorNodePushable {

            private IHyracksTaskContext ctx;

            private final int partition;

            public LeftOperator(IHyracksTaskContext ctx, int partition, RecordDescriptor inRecordDesc,
                    SortMergeIntervalJoinLocks locks) {
                this.ctx = ctx;
                this.partition = partition;
            }

            @Override
            public int getInputArity() {
                return inputArity;
            }

            @Override
            public IFrameWriter getInputFrameWriter(int index) {
                return new IFrameWriter() {
                    private SortMergeIntervalJoinTaskState state;

                    @Override
                    public void open() throws HyracksDataException {
                        locks.getLock(partition).lock();
                        try {
                            state = new SortMergeIntervalJoinTaskState(ctx.getJobletContext().getJobId(), new TaskId(
                                    getActivityId(), partition));
                            state.status.openLeft();
                            state.joiner = new SortMergeIntervalJoiner(state.status, locks, writer, recordDesc);
                            writer.open();
                            locks.getRight(partition).signal();
                        } finally {
                            locks.getLock(partition).unlock();
                        }
                    }

                    @Override
                    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                        locks.getLock(partition).lock();
                        try {
                            state.joiner.setLeftFrame(buffer);
                            state.joiner.processMerge();
                        } finally {
                            locks.getLock(partition).unlock();
                        }
                    }

                    @Override
                    public void fail() throws HyracksDataException {
                        locks.getLock(partition).lock();
                        try {
                            state.failed = true;
                        } finally {
                            locks.getLock(partition).unlock();
                        }
                    }

                    @Override
                    public void close() throws HyracksDataException {
                        locks.getLock(partition).lock();
                        try {
                            if (state.failed) {
                                writer.fail();
                            } else {
                                state.joiner.processMerge();
                                writer.close();
                            }
                            state.status.closeLeft();
                        } finally {
                            locks.getLock(partition).unlock();
                        }
                    }
                };
            }
        }
    }

    private class RightActivityNode extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        private final ActivityId joinAid;

        private SortMergeIntervalJoinLocks locks;

        public RightActivityNode(ActivityId id, ActivityId joinAid, SortMergeIntervalJoinLocks locks) {
            super(id);
            this.joinAid = joinAid;
            this.locks = locks;
        }

        @Override
        public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions)
                throws HyracksDataException {
            locks.setPartitions(nPartitions);
            RecordDescriptor inRecordDesc = recordDescProvider.getInputRecordDescriptor(getActivityId(), 0);
            return new RightOperator(ctx, partition, inRecordDesc, locks);
        }

        private class RightOperator extends AbstractUnaryOutputOperatorNodePushable {

            private int partition;

            private IHyracksTaskContext ctx;

            public RightOperator(IHyracksTaskContext ctx, int partition, RecordDescriptor inRecordDesc,
                    SortMergeIntervalJoinLocks locks) {
                this.ctx = ctx;
                this.partition = partition;
                this.recordDesc = inRecordDesc;
            }

            @Override
            public int getInputArity() {
                return inputArity;
            }

            @Override
            public IFrameWriter getInputFrameWriter(int index) {
                return new IFrameWriter() {
                    private SortMergeIntervalJoinTaskState state;

                    @Override
                    public void open() throws HyracksDataException {
                        locks.getLock(partition).lock();
                        try {
                            do {
                                // Wait for the state to be set in the context form Left.
                                state = (SortMergeIntervalJoinTaskState) ctx.getStateObject(new TaskId(joinAid,
                                        partition));
                                if (state == null) {
                                    locks.getRight(partition).await();
                                }
                            } while (state == null);
                            state.joiner.setRightRecordDescriptor(recordDesc);
                            state.status.openRight();
                        } catch (InterruptedException e) {
                            throw new HyracksDataException("RightOperator interrupted exceptrion", e);
                        } finally {
                            locks.getLock(partition).unlock();
                        }
                    }

                    @Override
                    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                        locks.getLock(partition).lock();
                        try {
                            state.joiner.setRightFrame(buffer);
                        } finally {
                            locks.getLock(partition).unlock();
                        }
                    }

                    @Override
                    public void fail() throws HyracksDataException {
                        locks.getLock(partition).lock();
                        try {
                            state.failed = true;
                        } finally {
                            locks.getLock(partition).unlock();
                        }
                    }

                    @Override
                    public void close() throws HyracksDataException {
                        locks.getLock(partition).lock();
                        try {
                            state.status.closeRight();
                        } finally {
                            locks.getLock(partition).unlock();
                        }
                    }
                };
            }
        }
    }

}
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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.logging.Logger;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.ITuplePartitionComputer;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.dataflow.std.join.SpillablePartitions;

/**
 * @author prestonc
 *         Holds tuples in separate partitions.
 *         The class attempts to keep them in memory and spill to disk if necessary.
 */
public class IntervalPartitionSpillablePartitions extends SpillablePartitions {

    private static final Logger LOGGER = Logger.getLogger(IntervalPartitionSpillablePartitions.class.getName());

    public IntervalPartitionSpillablePartitions(IHyracksTaskContext ctx, int memForJoin, int numOfPartitions,
            String runfileName, RecordDescriptor buildRd, ITuplePartitionComputer buildHpc, RecordDescriptor probeRd,
            ITuplePartitionComputer probeHpc) {
        super(ctx, numOfPartitions, numOfPartitions, runfileName, buildRd, buildHpc, probeRd, probeHpc);
    }

    @Override
    protected int selectPartitionToSpill() {
        int maxSize = -1;
        int partitionToSpill = -1;
        for (int i = 0; i < buildPSizeInTups.length; i++) { //Find the largest partition, to spill
            if (!hasSpilledPartition(i) && (buildPSizeInTups[i] > maxSize)) {
                maxSize = buildPSizeInTups[i];
                partitionToSpill = i;
            }
        }
        return partitionToSpill;
    }

    @Override
    protected ArrayList<Integer> selectPartitionsToReload() {
        ArrayList<Integer> p = new ArrayList<Integer>();
        int freeFrames = getFreeFrameCount();
        for (int i = nextSpilledPartition(0); i >= 0; i = nextSpilledPartition(i + 1)) {
            if (getBuildPartitionSizeInFrames(i) > 0 && (freeFrames - getBuildPartitionSizeInFrames(i) >= 0)) {
                p.add(i);
                freeFrames -= getBuildPartitionSizeInFrames(i);
            }
            if (freeFrames < 1) { //No more free buffer available
                return p;
            }
        }
        return p;
    }

    /*
     * Simple iterator for getting all a single partitions frames out of memory.
     */
    private int interatorFrameId = END_OF_PARTITION;

    public void memoryIteratorReset(int pid) {
        interatorFrameId = curPBuff[pid];
    }

    public boolean memoryIteratorHasNext() {
        return (interatorFrameId != END_OF_PARTITION);
    }

    public ByteBuffer memoryIteratorNext() {
        if (memoryIteratorHasNext()) {
            int next = interatorFrameId;
            interatorFrameId = nextBuff[interatorFrameId];
            return memBuffs[next].getBuffer();
        }
        return null;
    }

}

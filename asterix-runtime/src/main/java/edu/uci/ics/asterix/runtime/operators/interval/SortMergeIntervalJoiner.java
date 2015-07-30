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

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;

public class SortMergeIntervalJoiner {

    private final FrameTupleAccessor accessorLeft;
    private FrameTupleAccessor accessorRight;

    private SortMergeIntervalJoinLocks locks;
    private SortMergeIntervalStatus status;

    private int totalMemoryTuples = 0;
    private IFrameWriter writer;

    private ByteBuffer leftBuffer;
    private ByteBuffer rightBuffer;
    private int leftTupleIndex;
    private int rightTupleIndex;

    public SortMergeIntervalJoiner(SortMergeIntervalStatus status, SortMergeIntervalJoinLocks locks,
            IFrameWriter writer, RecordDescriptor leftRd) {
        this.status = status;
        this.locks = locks;
        this.writer = writer;

        this.accessorLeft = new FrameTupleAccessor(leftRd);
    }

    private boolean addToMemory(int i) {
        return true;
    }

    private boolean addToResult(int i, int j) {
        return true;
    }

    private boolean addToRunFile(int i) {
        return true;
    }

    private int compare(int i, int j) {
        return -1;
    }

    private void flushMemory() {

    }

    private int getLeftTuple() {
        // Get tuple from Left stream
        return 0;
    }

    private int getMemoryTuple(int index) {
        return index;
    }

    private int getRightTuple() {
        if (status.processingRunFile) {
            // Get next run file tuple
        } else {
            // Get tuple from Right stream
        }
        return 0;
    }

    private void incrementLeftTuple() {
        leftTupleIndex++;
    }

    private void incrementRighTuple() {
        rightTupleIndex++;
    }

    // memory management
    private boolean memoryHasTuples() {
        return false;
    }

    public void processMerge() {
        int rightTuple = 0;
        int memoryTuple = 0;
        int leftTuple = 0;

        if (status.reloadingLeftFrame) {
            // Get right tuple
            rightTuple = getRightTuple();

            if (status.fixedMemory) {
                // Write right tuple to run file
                addToRunFile(rightTuple);
            }

            for (int i = 0; i < totalMemoryTuples; ++i) {
                memoryTuple = getMemoryTuple(i);
                int c = compare(memoryTuple, rightTuple);
                if (c < 0) {
                    // remove from memory
                }
                if (c == 0) {
                    // add to result
                    addToResult(memoryTuple, rightTuple);
                }
            }

            if (!memoryHasTuples()) {
                //
                status.savingToRunFile = true;
                flushMemory();
                //            enter run file state
                //            reset memory state
            }

        }

        if (!status.fixedMemory) {
            leftTuple = getLeftTuple();
            int c = compare(leftTuple, rightTuple);
            while (c <= 0) {
                if (c == 0) {
                    // add to result
                    addToResult(leftTuple, rightTuple);
                    // append to memory
                    if (!addToMemory(leftTuple)) {
                        // go to fixed memory state
                        status.fixedMemory = true;
                        // write right tuple to run file
                        addToRunFile(rightTuple);
                        // break (do not increment left tuple)
                        break;
                    }
                }
            }
            incrementLeftTuple();
            leftTuple = getLeftTuple();
        }
        incrementRighTuple();

    }

    public void setLeftFrame(ByteBuffer buffer) {
        leftBuffer = buffer;
    }

    public void setRightFrame(ByteBuffer buffer) {
        rightBuffer = buffer;
    }

    public void setRightRecordDescriptor(RecordDescriptor rightRd) {
        this.accessorRight = new FrameTupleAccessor(rightRd);
    }
}

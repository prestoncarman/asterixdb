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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.comm.VSizeFrame;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTuplePairComparator;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileWriter;
import edu.uci.ics.hyracks.dataflow.std.sort.buffermanager.IFramePool;
import edu.uci.ics.hyracks.dataflow.std.sort.buffermanager.IFrameTupleBufferAccessor;
import edu.uci.ics.hyracks.dataflow.std.sort.buffermanager.ITupleBufferAccessor;
import edu.uci.ics.hyracks.dataflow.std.sort.buffermanager.ITupleBufferManager;
import edu.uci.ics.hyracks.dataflow.std.sort.buffermanager.VariableFramePool;
import edu.uci.ics.hyracks.dataflow.std.sort.buffermanager.VariableTupleMemoryManager;
import edu.uci.ics.hyracks.dataflow.std.structures.TuplePointer;

public class SortMergeIntervalJoiner {

    private static final int MEMORY_INDEX = -1;

    private final FrameTupleAccessor accessorLeft;
    private FrameTupleAccessor accessorRight;

    private SortMergeIntervalJoinLocks locks;
    private SortMergeIntervalStatus status;

    private IFrameWriter writer;

    private ByteBuffer leftBuffer;
    private ByteBuffer rightBuffer;
    private int leftTupleIndex;
    private int rightTupleIndex;

    private ITupleBufferManager bufferManager;
    private List<TuplePointer> memoryTuples;
    private IFrameTupleBufferAccessor memoryAccessor;

    private ByteBuffer runFileBuffer;
    private FrameTupleAppender runFileAppender;
    private final RunFileWriter runFileWriter;
    private ITupleBufferAccessor runFileAccessor;

    private FrameTupleAppender resultAppender;

    private FrameTuplePairComparator comparator;

    public SortMergeIntervalJoiner(IHyracksTaskContext ctx, int memorySize, SortMergeIntervalStatus status,
            SortMergeIntervalJoinLocks locks, FrameTuplePairComparator comparator, IFrameWriter writer,
            RecordDescriptor leftRd) throws HyracksDataException {
        this.status = status;
        this.locks = locks;
        this.writer = writer;
        this.comparator = comparator;

        accessorLeft = new FrameTupleAccessor(leftRd);

        // Memory
        IFramePool framePool = new VariableFramePool(ctx, (memorySize - 1) * ctx.getInitialFrameSize());
        bufferManager = new VariableTupleMemoryManager(framePool, leftRd);
        memoryTuples = new ArrayList<TuplePointer>();
        memoryAccessor = bufferManager.getFrameTupleAccessor();

        // Run File and frame cache
        FileReference file = ctx.getJobletContext().createManagedWorkspaceFile(
                this.getClass().getSimpleName() + this.toString());
        runFileWriter = new RunFileWriter(file, ctx.getIOManager());
        runFileWriter.open();
        runFileBuffer = ctx.allocateFrame(ctx.getInitialFrameSize());
        runFileAppender = new FrameTupleAppender(new VSizeFrame(ctx));

        // Result
        resultAppender = new FrameTupleAppender(new VSizeFrame(ctx));
    }

    private boolean addToMemory(IFrameTupleAccessor accessor, int idx) throws HyracksDataException {
        TuplePointer tuplePointer = new TuplePointer();
        if (bufferManager.insertTuple(accessor, idx, tuplePointer)) {
            memoryTuples.add(tuplePointer);
            return true;
        }
        return false;
    }

    private void addToResult(IFrameTupleAccessor accessor1, int index1, IFrameTupleAccessor accessor2, int index2)
            throws HyracksDataException {
        FrameUtils.appendConcatToWriter(writer, resultAppender, accessor1, index1, accessor2, index2);
    }

    private void addToRunFile(IFrameTupleAccessor accessor, int idx) throws HyracksDataException {
        if (!runFileAppender.append(accessor, idx)) {
            runFileAppender.flush(runFileWriter, true);
            runFileAppender.append(accessor, idx);
        }
    }

    private void flushMemory() throws HyracksDataException {
        bufferManager.reset();
        memoryTuples.clear();
    }

    private void incrementLeftTuple() {
        leftTupleIndex++;
    }

    private void incrementRighTuple() {
        rightTupleIndex++;
    }

    // memory management
    private boolean memoryHasTuples() {
        return bufferManager.getNumTuples() > 0;
    }

    public void processMerge() throws HyracksDataException {
        if (status.reloadingLeftFrame) {
            status.reloadingLeftFrame = false;
        } else {
            // Get right tuple
            if (status.processingRunFile) {
                // Get next run file tuple
                // TODO Read from disk
                accessorRight.reset(runFileBuffer);
            } else {
                // Get tuple from Right stream
                accessorRight.reset(rightBuffer);
            }

            if (status.fixedMemory) {
                // Write right tuple to run file
                addToRunFile(accessorRight, rightTupleIndex);
            }

            for (Iterator<TuplePointer> memoryIterator = memoryTuples.iterator(); memoryIterator.hasNext();) {
                TuplePointer tp = memoryIterator.next();
                memoryAccessor.reset(tp);
                int c = comparator.compare(memoryAccessor, MEMORY_INDEX, accessorRight, rightTupleIndex);
                if (c < 0) {
                    // remove from memory
                    bufferManager.deleteTuple(tp);
                    memoryIterator.remove();
                }
                if (c == 0) {
                    // add to result
                    addToResult(memoryAccessor, MEMORY_INDEX, accessorRight, rightTupleIndex);
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
            int c = comparator.compare(accessorLeft, leftTupleIndex, accessorRight, rightTupleIndex);
            while (c <= 0) {
                if (c == 0) {
                    // add to result
                    addToResult(accessorLeft, leftTupleIndex, accessorRight, rightTupleIndex);
                    // append to memory
                    if (!addToMemory(accessorLeft, leftTupleIndex)) {
                        // go to fixed memory state
                        status.fixedMemory = true;
                        // write right tuple to run file
                        addToRunFile(accessorRight, rightTupleIndex);
                        // break (do not increment left tuple)
                        break;
                    }
                }
            }
            incrementLeftTuple();
        }
        incrementRighTuple();
    }

    public void setLeftFrame(ByteBuffer buffer) {
        leftBuffer = buffer;
        accessorLeft.reset(leftBuffer);
    }

    public void setRightFrame(ByteBuffer buffer) {
        rightBuffer = buffer;
        accessorRight.reset(rightBuffer);
    }

    public void setRightRecordDescriptor(RecordDescriptor rightRd) {
        accessorRight = new FrameTupleAccessor(rightRd);
    }
}

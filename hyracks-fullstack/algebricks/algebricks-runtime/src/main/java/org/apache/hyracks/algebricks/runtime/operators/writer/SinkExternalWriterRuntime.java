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
package org.apache.hyracks.algebricks.runtime.operators.writer;

import java.nio.ByteBuffer;

import org.apache.hyracks.algebricks.runtime.operators.base.AbstractOneInputSinkPushRuntime;
import org.apache.hyracks.algebricks.runtime.writers.IExternalWriter;
import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

final class SinkExternalWriterRuntime extends AbstractOneInputSinkPushRuntime {
    private final int sourceColumn;
    private final IWriterPartitioner partitioner;
    private final IPointable sourceValue;
    private final IExternalWriter writer;
    private FrameTupleAccessor tupleAccessor;
    private FrameTupleReference tupleRef;
    private IFrameWriter frameWriter;

    SinkExternalWriterRuntime(int sourceColumn, IWriterPartitioner partitioner, RecordDescriptor inputRecordDesc,
            IExternalWriter writer) {
        this.sourceColumn = sourceColumn;
        this.partitioner = partitioner;
        this.sourceValue = new VoidPointable();
        this.inputRecordDesc = inputRecordDesc;
        this.writer = writer;
    }

    @Override
    public void open() throws HyracksDataException {
        if (tupleAccessor == null) {
            writer.open();
            tupleAccessor = new FrameTupleAccessor(inputRecordDesc);
            tupleRef = new FrameTupleReference();
        }
        this.frameWriter.open();
    }

    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        tupleAccessor.reset(buffer);
        for (int i = 0; i < tupleAccessor.getTupleCount(); i++) {
            tupleRef.reset(tupleAccessor, i);
            if (isNewPartition(i)) {
                writer.initNewPartition(tupleRef);
            }
            setValue(tupleRef, sourceColumn, sourceValue);
            writer.write(sourceValue);
        }
    }

    @Override
    public void fail() throws HyracksDataException {
        writer.abort();
        frameWriter.fail();
    }

    @Override
    public void close() throws HyracksDataException {
        writer.close();
        frameWriter.close();
    }

    @Override
    public void setOutputFrameWriter(int index, IFrameWriter frameWriter, RecordDescriptor recordDesc) {
        this.frameWriter = frameWriter;
    }

    private boolean isNewPartition(int index) throws HyracksDataException {
        return partitioner.isNewPartition(tupleAccessor, index);
    }

    private void setValue(IFrameTupleReference tuple, int column, IPointable value) {
        byte[] data = tuple.getFieldData(column);
        int start = tuple.getFieldStart(column);
        int length = tuple.getFieldLength(column);
        value.set(data, start, length);
    }
}

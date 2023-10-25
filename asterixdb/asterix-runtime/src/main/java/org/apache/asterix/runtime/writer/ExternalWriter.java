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
package org.apache.asterix.runtime.writer;

import org.apache.hyracks.algebricks.runtime.writers.IExternalWriter;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

final class ExternalWriter implements IExternalWriter {
    private final IPathResolver pathResolver;
    private final IExternalFileWriter writer;
    private final int maxResultPerFile;
    private int tupleCounter;

    public ExternalWriter(IPathResolver pathResolver, IExternalFileWriter writer, int maxResultPerFile) {
        this.pathResolver = pathResolver;
        this.writer = writer;
        this.maxResultPerFile = maxResultPerFile;
    }

    @Override
    public void open() throws HyracksDataException {
        writer.open();
    }

    @Override
    public void initNewPartition(IFrameTupleReference tuple) throws HyracksDataException {
        tupleCounter = 0;
        writer.newFile(pathResolver.getPartitionPath(tuple));
    }

    @Override
    public void write(IValueReference value) throws HyracksDataException {
        writer.write(value);
        tupleCounter++;
        if (tupleCounter >= maxResultPerFile) {
            tupleCounter = 0;
            writer.newFile(pathResolver.getNextPath());
        }
    }

    @Override
    public void abort() throws HyracksDataException {
        writer.abort();
    }

    @Override
    public void close() throws HyracksDataException {
        writer.close();
    }
}

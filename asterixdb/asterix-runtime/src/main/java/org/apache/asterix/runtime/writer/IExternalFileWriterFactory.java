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

import java.io.Serializable;

import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * An interface for writing to a storage device
 * Implementer should also provide a singleton to {@link IExternalFileFilterWriterFactoryProvider}
 */
public interface IExternalFileWriterFactory extends Serializable {
    /**
     * Create a writer
     *
     * @param context        task context
     * @param printerFactory printer factory for writing the final result
     * @return a new file writer
     */
    IExternalFileWriter createWriter(IHyracksTaskContext context, IExternalFilePrinterFactory printerFactory)
            throws HyracksDataException;

    /**
     * @return file (or path) separator
     */
    char getFileSeparator();

    /**
     * Validate the writer by running a test write routine to ensure the writer has the appropriate permissions
     */
    void validate() throws AlgebricksException;
}

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
package org.apache.asterix.replication.messaging;

import java.io.DataInput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.logging.Logger;

import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.exceptions.ReplicationException;
import org.apache.asterix.common.replication.IReplicationThread;
import org.apache.asterix.replication.api.IReplicaTask;
import org.apache.asterix.replication.functions.ReplicationProtocol;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IIOManager;

/**
 * A task to delete a file on a replica if exists
 */
public class DeleteFileTask implements IReplicaTask {

    private static final Logger LOGGER = Logger.getLogger(DeleteFileTask.class.getName());
    private final String file;

    public DeleteFileTask(String file) {
        this.file = file;
    }

    @Override
    public void perform(INcApplicationContext appCtx, IReplicationThread worker) {
        try {
            final IIOManager ioManager = appCtx.getIoManager();
            final File localFile = ioManager.resolve(file).getFile();
            if (localFile.exists()) {
                Files.delete(localFile.toPath());
                LOGGER.info(() -> "Deleted file: " + localFile.getAbsolutePath());
            } else {
                LOGGER.warning(() -> "Requested to delete a non-existing file: " + localFile.getAbsolutePath());
            }
            ReplicationProtocol.sendAck(worker.getChannel(), worker.getReusableBuffer());
        } catch (IOException e) {
            throw new ReplicationException(e);
        }
    }

    @Override
    public ReplicationProtocol.ReplicationRequestType getMessageType() {
        return ReplicationProtocol.ReplicationRequestType.DELETE_RESOURCE_FILE;
    }

    @Override
    public void serialize(OutputStream out) throws HyracksDataException {
        try {
            DataOutputStream dos = new DataOutputStream(out);
            dos.writeUTF(file);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    public static DeleteFileTask create(DataInput input) throws IOException {
        return new DeleteFileTask(input.readUTF());
    }
}
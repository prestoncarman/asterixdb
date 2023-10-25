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
package org.apache.asterix.cloud.writer;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Random;

import org.apache.asterix.cloud.CloudResettableInputStream;
import org.apache.asterix.cloud.WriterSingleBufferProvider;
import org.apache.asterix.cloud.clients.ICloudBufferedWriter;
import org.apache.asterix.cloud.clients.ICloudClient;
import org.apache.asterix.cloud.clients.aws.s3.S3ClientConfig;
import org.apache.asterix.cloud.clients.aws.s3.S3CloudClient;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.aws.s3.S3Utils;
import org.apache.asterix.runtime.writer.IExternalFileFilterWriterFactoryProvider;
import org.apache.asterix.runtime.writer.IExternalFilePrinterFactory;
import org.apache.asterix.runtime.writer.IExternalFileWriter;
import org.apache.asterix.runtime.writer.IExternalFileWriterFactory;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.util.ExceptionUtils;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import software.amazon.awssdk.services.s3.model.NoSuchBucketException;

public final class S3ExternalFileWriterFactory implements IExternalFileWriterFactory {
    private static final Logger LOGGER = LogManager.getLogger();
    private static final long serialVersionUID = 4551318140901866805L;
    public static final IExternalFileFilterWriterFactoryProvider PROVIDER = S3ExternalFileWriterFactory::new;
    private final S3ClientConfig config;
    private final Map<String, String> configuration;
    private transient S3CloudClient cloudClient;

    private S3ExternalFileWriterFactory(Map<String, String> configuration) {
        this.config = S3ClientConfig.of(configuration);
        this.configuration = configuration;
        cloudClient = null;
    }

    @Override
    public IExternalFileWriter createWriter(IHyracksTaskContext context, IExternalFilePrinterFactory printerFactory)
            throws HyracksDataException {
        buildClient();
        String bucket = configuration.get(ExternalDataConstants.CONTAINER_NAME_FIELD_NAME);
        return new CloudExternalFileWriter(printerFactory.createPrinter(), cloudClient, bucket);
    }

    private void buildClient() throws HyracksDataException {
        try {
            synchronized (this) {
                if (cloudClient == null) {
                    // only a single client should be build
                    cloudClient = new S3CloudClient(config, S3Utils.buildAwsS3Client(configuration));
                }
            }
        } catch (CompilationException e) {
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public char getFileSeparator() {
        return '/';
    }

    @Override
    public void validate() throws AlgebricksException {
        ICloudClient testClient = new S3CloudClient(config, S3Utils.buildAwsS3Client(configuration));
        String bucket = configuration.get(ExternalDataConstants.CONTAINER_NAME_FIELD_NAME);
        if (bucket == null || bucket.isEmpty()) {
            throw new CompilationException(ErrorCode.PARAMETERS_REQUIRED,
                    ExternalDataConstants.CONTAINER_NAME_FIELD_NAME);
        }
        try {
            doValidate(testClient, bucket);
        } catch (IOException e) {
            if (e.getCause() instanceof NoSuchBucketException) {
                throw new CompilationException(ErrorCode.EXTERNAL_SOURCE_CONTAINER_NOT_FOUND, bucket);
            } else {
                LOGGER.fatal(e);
                throw CompilationException.create(ErrorCode.EXTERNAL_SOURCE_ERROR,
                        ExceptionUtils.getMessageOrToString(e));
            }
        }
    }

    private static void doValidate(ICloudClient testClient, String bucket) throws IOException {
        Random random = new Random();
        String pathPrefix = "testFile";
        String path = pathPrefix + random.nextInt();
        while (testClient.exists(bucket, path)) {
            path = pathPrefix + random.nextInt();
        }

        long writeValue = random.nextLong();
        byte[] data = new byte[Long.BYTES];
        LongPointable.setLong(data, 0, writeValue);
        ICloudBufferedWriter writer = testClient.createBufferedWriter(bucket, path);
        CloudResettableInputStream stream = null;
        boolean aborted = false;
        try {
            stream = new CloudResettableInputStream(writer, new WriterSingleBufferProvider());
            stream.write(data, 0, data.length);
        } catch (HyracksDataException e) {
            stream.abort();
        } finally {
            if (stream != null && !aborted) {
                stream.finish();
                stream.close();
            }
        }

        try {
            long readValue = LongPointable.getLong(testClient.readAllBytes(bucket, path), 0);
            if (writeValue != readValue) {
                // This should never happen unless S3 is messed up. But log for sanity check
                LOGGER.warn(
                        "The writer can write but the written values wasn't successfully read back (wrote: {}, read:{})",
                        writeValue, readValue);
            }
        } finally {
            // Delete the written file
            testClient.deleteObjects(bucket, Collections.singleton(path));
        }
    }
}

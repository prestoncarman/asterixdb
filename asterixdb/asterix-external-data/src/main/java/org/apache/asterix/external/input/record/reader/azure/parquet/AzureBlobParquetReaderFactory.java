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
package org.apache.asterix.external.input.record.reader.azure.parquet;

import static org.apache.asterix.external.util.azure.blob_storage.AzureConstants.HADOOP_AZURE_BLOB_PROTOCOL;
import static org.apache.asterix.external.util.azure.blob_storage.AzureUtils.buildAzureBlobClient;
import static org.apache.asterix.external.util.azure.blob_storage.AzureUtils.configureAzureHdfsJobConf;
import static org.apache.asterix.external.util.azure.blob_storage.AzureUtils.listBlobItems;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.asterix.common.api.IApplicationContext;
import org.apache.asterix.common.external.IExternalFilterEvaluator;
import org.apache.asterix.common.external.IExternalFilterEvaluatorFactory;
import org.apache.asterix.external.input.HDFSDataSourceFactory;
import org.apache.asterix.external.input.record.reader.abstracts.AbstractExternalInputStreamFactory.IncludeExcludeMatcher;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.ExternalDataPrefix;
import org.apache.asterix.external.util.ExternalDataUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.application.IServiceContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.IWarningCollector;

import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlobItem;

public class AzureBlobParquetReaderFactory extends HDFSDataSourceFactory {
    private static final long serialVersionUID = -6140824803254158253L;
    private static final List<String> recordReaderNames =
            Collections.singletonList(ExternalDataConstants.KEY_ADAPTER_NAME_AZURE_BLOB);

    @Override
    public void configure(IServiceContext serviceCtx, Map<String, String> configuration,
            IWarningCollector warningCollector, IExternalFilterEvaluatorFactory filterEvaluatorFactory)
            throws AlgebricksException, HyracksDataException {
        IApplicationContext appCtx = (IApplicationContext) serviceCtx.getApplicationContext();

        // get endpoint
        BlobServiceClient blobServiceClient = buildAzureBlobClient(appCtx, configuration);
        String endPoint = extractEndPoint(blobServiceClient.getAccountUrl());

        // get include/exclude matchers
        IncludeExcludeMatcher includeExcludeMatcher = ExternalDataUtils.getIncludeExcludeMatchers(configuration);

        // prepare prefix for computed field calculations
        IExternalFilterEvaluator evaluator = filterEvaluatorFactory.create(serviceCtx, warningCollector);
        ExternalDataPrefix externalDataPrefix = new ExternalDataPrefix(configuration);
        configuration.put(ExternalDataPrefix.PREFIX_ROOT_FIELD_NAME, externalDataPrefix.getRoot());

        List<BlobItem> filesOnly = listBlobItems(blobServiceClient, configuration, includeExcludeMatcher,
                warningCollector, externalDataPrefix, evaluator);

        // get path
        String container = configuration.get(ExternalDataConstants.CONTAINER_NAME_FIELD_NAME);
        String path = buildPathURIs(container, filesOnly, endPoint);

        // put Azure configurations to AsterixDB's Hadoop configuration
        putAzureBlobConfToHadoopConf(configuration, path);

        // configure Hadoop Azure input splits
        JobConf conf = prepareHDFSConf(serviceCtx, configuration, filterEvaluatorFactory);
        configureAzureHdfsJobConf(conf, configuration, endPoint);
        configureHdfsConf(conf, configuration);
    }

    @Override
    public List<String> getRecordReaderNames() {
        return recordReaderNames;
    }

    @Override
    public Set<String> getReaderSupportedFormats() {
        return Collections.singleton(ExternalDataConstants.FORMAT_PARQUET);
    }

    /**
     * Prepare Hadoop configurations to read parquet files
     *
     * @param path Comma-delimited paths
     */
    private static void putAzureBlobConfToHadoopConf(Map<String, String> configuration, String path) {
        configuration.put(ExternalDataConstants.KEY_PATH, path);
        configuration.put(ExternalDataConstants.KEY_INPUT_FORMAT, ExternalDataConstants.INPUT_FORMAT_PARQUET);
        configuration.put(ExternalDataConstants.KEY_PARSER, ExternalDataConstants.FORMAT_NOOP);
    }

    /**
     * Build Azure Blob Storage path-style for the requested files
     *
     * @param container container
     * @param filesOnly files
     * @param endPoint  endpoint
     * @return Comma-delimited paths (e.g., "wasbs://container@accountName.blob.core.windows.net/file1.parquet,
     * wasbs://container@accountName.blob.core.windows.net/file2.parquet")
     */
    private static String buildPathURIs(String container, List<BlobItem> filesOnly, String endPoint) {
        StringBuilder builder = new StringBuilder();

        if (!filesOnly.isEmpty()) {
            appendFileURI(builder, container, endPoint, filesOnly.get(0));
            for (int i = 1; i < filesOnly.size(); i++) {
                builder.append(',');
                appendFileURI(builder, container, endPoint, filesOnly.get(i));
            }
        }

        return builder.toString();
    }

    private static String extractEndPoint(String uri) {
        //The URI is in the form http(s)://<accountName>.blob.core.windows.net
        //We need to Remove the protocol (i.e., http(s)://) from the URI
        return uri.substring(uri.indexOf("//") + "//".length());
    }

    private static void appendFileURI(StringBuilder builder, String container, String endPoint, BlobItem file) {
        builder.append(HADOOP_AZURE_BLOB_PROTOCOL);
        builder.append("://");
        builder.append(container);
        builder.append('@');
        builder.append(endPoint);
        builder.append('/');
        builder.append(file.getName());
    }
}

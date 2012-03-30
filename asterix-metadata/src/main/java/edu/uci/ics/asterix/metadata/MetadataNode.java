/*
 * Copyright 2009-2010 by The Regents of the University of California
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

package edu.uci.ics.asterix.metadata;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import edu.uci.ics.asterix.common.config.DatasetConfig.DatasetType;
import edu.uci.ics.asterix.common.config.DatasetConfig.IndexType;
import edu.uci.ics.asterix.common.context.INodeApplicationState;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.metadata.api.IMetadataIndex;
import edu.uci.ics.asterix.metadata.api.IMetadataNode;
import edu.uci.ics.asterix.metadata.api.IValueExtractor;
import edu.uci.ics.asterix.metadata.bootstrap.MetadataPrimaryIndexes;
import edu.uci.ics.asterix.metadata.bootstrap.MetadataSecondaryIndexes;
import edu.uci.ics.asterix.metadata.entities.Dataset;
import edu.uci.ics.asterix.metadata.entities.Datatype;
import edu.uci.ics.asterix.metadata.entities.Dataverse;
import edu.uci.ics.asterix.metadata.entities.Function;
import edu.uci.ics.asterix.metadata.entities.Index;
import edu.uci.ics.asterix.metadata.entities.InternalDatasetDetails;
import edu.uci.ics.asterix.metadata.entities.Node;
import edu.uci.ics.asterix.metadata.entities.NodeGroup;
import edu.uci.ics.asterix.metadata.entitytupletranslators.DatasetTupleTranslator;
import edu.uci.ics.asterix.metadata.entitytupletranslators.DatatypeTupleTranslator;
import edu.uci.ics.asterix.metadata.entitytupletranslators.DataverseTupleTranslator;
import edu.uci.ics.asterix.metadata.entitytupletranslators.FunctionTupleTranslator;
import edu.uci.ics.asterix.metadata.entitytupletranslators.IndexTupleTranslator;
import edu.uci.ics.asterix.metadata.entitytupletranslators.NodeGroupTupleTranslator;
import edu.uci.ics.asterix.metadata.entitytupletranslators.NodeTupleTranslator;
import edu.uci.ics.asterix.metadata.valueextractors.DatasetNameValueExtractor;
import edu.uci.ics.asterix.metadata.valueextractors.DatatypeNameValueExtractor;
import edu.uci.ics.asterix.metadata.valueextractors.MetadataEntityValueExtractor;
import edu.uci.ics.asterix.metadata.valueextractors.NestedDatatypeNameValueExtractor;
import edu.uci.ics.asterix.metadata.valueextractors.TupleCopyValueExtractor;
import edu.uci.ics.asterix.transaction.management.exception.ACIDException;
import edu.uci.ics.asterix.transaction.management.service.logging.DataUtil;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionContext;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionManagementConstants.LockManagerConstants.LockMode;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionProvider;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.util.TupleUtils;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangePredicate;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndex;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IndexRegistry;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOp;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;

public class MetadataNode implements IMetadataNode {
    private static final long serialVersionUID = 1L;

    private static final int METADATANODE_RESOURCE_ID = 0;
    private static final byte[] metadataResourceId = DataUtil.intToByteArray(METADATANODE_RESOURCE_ID);

    private final IndexRegistry<IIndex> indexRegistry;
    private final TransactionProvider transactionProvider;
    private final AtomicInteger resourceIdSeed;

    public static MetadataNode INSTANCE;

    public MetadataNode(INodeApplicationState applicationState) {
        super();
        this.transactionProvider = applicationState.getTransactionProvider();
        this.indexRegistry = applicationState.getApplicationRuntimeContext().getIndexRegistry();
        this.resourceIdSeed = new AtomicInteger();
    }

    @Override
    public void beginTransaction(long transactionId) throws ACIDException, RemoteException {
        transactionProvider.getTransactionManager().beginTransaction(transactionId);
    }

    @Override
    public void commitTransaction(long txnId) throws RemoteException, ACIDException {
        TransactionContext txnCtx = transactionProvider.getTransactionManager().getTransactionContext(txnId);
        transactionProvider.getTransactionManager().commitTransaction(txnCtx);
    }

    @Override
    public void abortTransaction(long txnId) throws RemoteException, ACIDException {
        try {
            TransactionContext txnCtx = transactionProvider.getTransactionManager().getTransactionContext(txnId);
            transactionProvider.getTransactionManager().abortTransaction(txnCtx);
        } catch (ACIDException e) {
            e.printStackTrace();
            throw e;
        }
    }

    @Override
    public boolean lock(long txnId, int lockMode) throws ACIDException, RemoteException {
        TransactionContext txnCtx = transactionProvider.getTransactionManager().getTransactionContext(txnId);
        return transactionProvider.getLockManager().lock(txnCtx, metadataResourceId, lockMode);
    }

    @Override
    public boolean unlock(long txnId) throws ACIDException, RemoteException {
        TransactionContext txnCtx = transactionProvider.getTransactionManager().getTransactionContext(txnId);
        return transactionProvider.getLockManager().unlock(txnCtx, metadataResourceId);
    }

    @Override
    public void addDataverse(long txnId, Dataverse dataverse) throws MetadataException, RemoteException {
        // TODO: This will become much simpler once the LSM-BTree supports a true 'insert' operation
        // Check if the dataverse exists already
        if (getDataverse(txnId, dataverse.getDataverseName()) != null) {
            throw new MetadataException("A dataverse with this name " + dataverse.getDataverseName()
                    + " already exists.");
        }

        // The dataverse does not already exist, so insert the dataverse
        try {
            DataverseTupleTranslator tupleReaderWriter = new DataverseTupleTranslator(true);
            ITupleReference tuple = tupleReaderWriter.getTupleFromMetadataEntity(dataverse);
            insertTupleIntoIndex(txnId, MetadataPrimaryIndexes.DATAVERSE_DATASET, tuple);
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public void addDataset(long txnId, Dataset dataset) throws MetadataException, RemoteException {
        // TODO: This will become much simpler once the LSM-BTree supports a true 'insert' operation
        // Check if the dataset exists already
        if (getDataset(txnId, dataset.getDataverseName(), dataset.getDatasetName()) != null) {
            throw new MetadataException("A dataset with this name " + dataset.getDatasetName()
                    + " already exists in dataverse '" + dataset.getDataverseName() + "'.");
        }

        // The dataset does not already exist, so insert the dataset
        try {
            // Insert into the 'dataset' dataset.
            DatasetTupleTranslator tupleReaderWriter = new DatasetTupleTranslator(true);
            ITupleReference datasetTuple = tupleReaderWriter.getTupleFromMetadataEntity(dataset);
            insertTupleIntoIndex(txnId, MetadataPrimaryIndexes.DATASET_DATASET, datasetTuple);
            if (dataset.getType() == DatasetType.INTERNAL || dataset.getType() == DatasetType.FEED) {
                // Add the primary index for the dataset.
                InternalDatasetDetails id = (InternalDatasetDetails) dataset.getDatasetDetails();
                Index primaryIndex = new Index(dataset.getDataverseName(), dataset.getDatasetName(),
                        dataset.getDatasetName(), IndexType.LSM_BTREE, id.getPrimaryKey(), true,
                        MetadataManager.INSTANCE.generateResourceId());
                addIndex(txnId, primaryIndex);
                ITupleReference nodeGroupTuple = createTuple(id.getNodeGroupName(), dataset.getDataverseName(),
                        dataset.getDatasetName());
                insertTupleIntoIndex(txnId, MetadataSecondaryIndexes.GROUPNAME_ON_DATASET_INDEX, nodeGroupTuple);
            }
            // Add entry in datatype secondary index.
            ITupleReference dataTypeTuple = createTuple(dataset.getDataverseName(), dataset.getDatatypeName(),
                    dataset.getDatasetName());
            insertTupleIntoIndex(txnId, MetadataSecondaryIndexes.DATATYPENAME_ON_DATASET_INDEX, dataTypeTuple);
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public void addIndex(long txnId, Index index) throws MetadataException, RemoteException {
        // TODO: This will become much simpler once the LSM-BTree supports a true 'insert' operation
        // Check if the index exists already
        if (getIndex(txnId, index.getDataverseName(), index.getDatasetName(), index.getIndexName()) != null) {
            throw new MetadataException("An index with name '" + index.getIndexName() + "' already exists.");
        }

        // The index does not already exist, so insert the index
        try {
            IndexTupleTranslator tupleWriter = new IndexTupleTranslator(true);
            ITupleReference tuple = tupleWriter.getTupleFromMetadataEntity(index);
            insertTupleIntoIndex(txnId, MetadataPrimaryIndexes.INDEX_DATASET, tuple);
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public void addNode(long txnId, Node node) throws MetadataException, RemoteException {
        // TODO: This will become much simpler once the LSM-BTree supports a true 'insert' operation
        // Check if the node exists already
        if (getNode(txnId, node.getNodeName()) != null) {
            throw new MetadataException("A node with name '" + node.getNodeName() + "' already exists.");
        }

        // The node does not already exist, so insert the node
        try {
            NodeTupleTranslator tupleReaderWriter = new NodeTupleTranslator(true);
            ITupleReference tuple = tupleReaderWriter.getTupleFromMetadataEntity(node);
            insertTupleIntoIndex(txnId, MetadataPrimaryIndexes.NODE_DATASET, tuple);
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public void addNodeGroup(long txnId, NodeGroup nodeGroup) throws MetadataException, RemoteException {
        // TODO: This will become much simpler once the LSM-BTree supports a true 'insert' operation
        // Check if the nodegroup exists already
        if (getNodeGroup(txnId, nodeGroup.getNodeGroupName()) != null) {
            throw new MetadataException("A nodegroup with name '" + nodeGroup.getNodeGroupName() + "' already exists.");
        }

        // The nodegroup does not already exist, so insert the nodegroup
        try {
            NodeGroupTupleTranslator tupleReaderWriter = new NodeGroupTupleTranslator(true);
            ITupleReference tuple = tupleReaderWriter.getTupleFromMetadataEntity(nodeGroup);
            insertTupleIntoIndex(txnId, MetadataPrimaryIndexes.NODEGROUP_DATASET, tuple);
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public void addDatatype(long txnId, Datatype datatype) throws MetadataException, RemoteException {
        // TODO: This will become much simpler once the LSM-BTree supports a true 'insert' operation
        // Check if the datatype exists already
        if (getDatatype(txnId, datatype.getDataverseName(), datatype.getDatatypeName()) != null) {
            throw new MetadataException("A datatype with name '" + datatype.getDatatypeName() + "' already exists.");
        }

        // The datatype does not already exist, so insert the datatype
        try {
            DatatypeTupleTranslator tupleReaderWriter = new DatatypeTupleTranslator(txnId, this, true);
            ITupleReference tuple = tupleReaderWriter.getTupleFromMetadataEntity(datatype);
            insertTupleIntoIndex(txnId, MetadataPrimaryIndexes.DATATYPE_DATASET, tuple);
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public void addFunction(long txnId, Function function) throws MetadataException, RemoteException {
        // TODO: This will become much simpler once the LSM-BTree supports a true 'insert' operation
        // Check if the function exists already
        if (getFunction(txnId, function.getDataverseName(), function.getFunctionName(), function.getFunctionArity()) != null) {
            throw new MetadataException("A dataset with this name " + function.getFunctionName() + " and arity "
                    + function.getFunctionArity() + " already exists in dataverse '" + function.getDataverseName()
                    + "'.");
        }

        // The function does not already exist, so insert the function
        try {
            // Insert into the 'function' dataset.
            FunctionTupleTranslator tupleReaderWriter = new FunctionTupleTranslator(true);
            ITupleReference functionTuple = tupleReaderWriter.getTupleFromMetadataEntity(function);
            insertTupleIntoIndex(txnId, MetadataPrimaryIndexes.FUNCTION_DATASET, functionTuple);
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    public void insertIntoDatatypeSecondaryIndex(long txnId, String dataverseName, String nestedTypeName,
            String topTypeName) throws Exception {
        ITupleReference tuple = createTuple(dataverseName, nestedTypeName, topTypeName);
        insertTupleIntoIndex(txnId, MetadataSecondaryIndexes.DATATYPENAME_ON_DATATYPE_INDEX, tuple);
    }

    private void insertTupleIntoIndex(long txnId, IMetadataIndex index, ITupleReference tuple) throws Exception {
        int fileId = index.getFileId();
        IIndex physicalIndex = indexRegistry.get(DataUtil.byteArrayToInt(index.getResourceId(), 0));
        physicalIndex.open(fileId);
        IIndexAccessor indexAccessor = physicalIndex.createAccessor();

        TransactionContext txnCtx = transactionProvider.getTransactionManager().getTransactionContext(txnId);
        transactionProvider.getLockManager().lock(txnCtx, index.getResourceId(), LockMode.EXCLUSIVE);

        indexAccessor.insert(tuple);
        index.getTreeLogger().generateLogRecord(transactionProvider, txnCtx, IndexOp.INSERT, tuple);
    }

    @Override
    public void dropDataverse(long txnId, String dataverseName) throws MetadataException, RemoteException {
        // TODO: This will become much simpler once the LSM-BTree supports a true 'delete' operation
        // Check if the dataverse exists
        if (getDataverse(txnId, dataverseName) == null) {
            throw new MetadataException("Cannot drop dataverse '" + dataverseName + "' because it doesn't exist.");
        }

        // The dataverse exists, so drop it
        try {
            List<Dataset> dataverseDatasets;
            // As a side effect, acquires an S lock on the 'dataset' dataset
            // on behalf of txnId.
            dataverseDatasets = getDataverseDatasets(txnId, dataverseName);
            if (dataverseDatasets != null && dataverseDatasets.size() > 0) {
                // Drop all datasets in this dataverse.
                for (int i = 0; i < dataverseDatasets.size(); i++) {
                    dropDataset(txnId, dataverseName, dataverseDatasets.get(i).getDatasetName());
                }
            }
            List<Datatype> dataverseDatatypes;
            // As a side effect, acquires an S lock on the 'datatype' dataset
            // on behalf of txnId.
            dataverseDatatypes = getDataverseDatatypes(txnId, dataverseName);
            if (dataverseDatatypes != null && dataverseDatatypes.size() > 0) {
                // Drop all types in this dataverse.
                for (int i = 0; i < dataverseDatatypes.size(); i++) {
                    forceDropDatatype(txnId, dataverseName, dataverseDatatypes.get(i).getDatatypeName());
                }
            }
            // Delete the dataverse entry from the 'dataverse' dataset.
            ITupleReference searchKey = createTuple(dataverseName);
            // As a side effect, acquires an S lock on the 'dataverse' dataset
            // on behalf of txnId.
            ITupleReference tuple = getTupleToBeDeleted(txnId, MetadataPrimaryIndexes.DATAVERSE_DATASET, searchKey);
            deleteTupleFromIndex(txnId, MetadataPrimaryIndexes.DATAVERSE_DATASET, tuple);
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public void dropDataset(long txnId, String dataverseName, String datasetName) throws MetadataException,
            RemoteException {
        // TODO: This will become much simpler once the LSM-BTree supports a true 'delete' operation
        // Check if the dataset exists already
        Dataset dataset;
        dataset = getDataset(txnId, dataverseName, datasetName);
        if (dataset == null) {
            throw new MetadataException("Cannot drop dataset '" + datasetName + "' because it doesn't exist.");
        }

        // The dataset exists, so drop it
        try {
            // Delete entry from the 'datasets' dataset.
            ITupleReference searchKey = createTuple(dataverseName, datasetName);
            // Searches the index for the tuple to be deleted. Acquires an S
            // lock on the 'dataset' dataset.
            ITupleReference datasetTuple = getTupleToBeDeleted(txnId, MetadataPrimaryIndexes.DATASET_DATASET, searchKey);
            deleteTupleFromIndex(txnId, MetadataPrimaryIndexes.DATASET_DATASET, datasetTuple);
            // Delete entry from secondary index 'group'.
            if (dataset.getType() == DatasetType.INTERNAL || dataset.getType() == DatasetType.FEED) {
                InternalDatasetDetails id = (InternalDatasetDetails) dataset.getDatasetDetails();
                ITupleReference groupNameSearchKey = createTuple(id.getNodeGroupName(), dataverseName, datasetName);
                // Searches the index for the tuple to be deleted. Acquires an S
                // lock on the GROUPNAME_ON_DATASET_INDEX index.
                ITupleReference groupNameTuple = getTupleToBeDeleted(txnId,
                        MetadataSecondaryIndexes.GROUPNAME_ON_DATASET_INDEX, groupNameSearchKey);
                deleteTupleFromIndex(txnId, MetadataSecondaryIndexes.GROUPNAME_ON_DATASET_INDEX, groupNameTuple);
            }
            // Delete entry from secondary index 'type'.
            ITupleReference dataTypeSearchKey = createTuple(dataverseName, dataset.getDatatypeName(), datasetName);
            // Searches the index for the tuple to be deleted. Acquires an S
            // lock on the DATATYPENAME_ON_DATASET_INDEX index.
            ITupleReference dataTypeTuple = getTupleToBeDeleted(txnId,
                    MetadataSecondaryIndexes.DATATYPENAME_ON_DATASET_INDEX, dataTypeSearchKey);
            deleteTupleFromIndex(txnId, MetadataSecondaryIndexes.DATATYPENAME_ON_DATASET_INDEX, dataTypeTuple);
            // Delete entry(s) from the 'indexes' dataset.
            if (dataset.getType() == DatasetType.INTERNAL || dataset.getType() == DatasetType.FEED) {
                List<Index> datasetIndexes = getDatasetIndexes(txnId, dataverseName, datasetName);
                for (Index index : datasetIndexes) {
                    dropIndex(txnId, dataverseName, datasetName, index.getIndexName());
                }
            }
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public void dropIndex(long txnId, String dataverseName, String datasetName, String indexName)
            throws MetadataException, RemoteException {
        // TODO: This will become much simpler once the LSM-BTree supports a true 'delete' operation
        // Check if the index exists already
        if (getIndex(txnId, dataverseName, datasetName, indexName) == null) {
            throw new MetadataException("Cannot drop index '" + datasetName + "." + indexName
                    + "' because it doesn't exist.");
        }

        // The index exists, so drop it
        try {
            ITupleReference searchKey = createTuple(dataverseName, datasetName, indexName);
            // Searches the index for the tuple to be deleted. Acquires an S
            // lock on the 'index' dataset.
            ITupleReference tuple = getTupleToBeDeleted(txnId, MetadataPrimaryIndexes.INDEX_DATASET, searchKey);
            deleteTupleFromIndex(txnId, MetadataPrimaryIndexes.INDEX_DATASET, tuple);
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public void dropNodegroup(long txnId, String nodeGroupName) throws MetadataException, RemoteException {
        // TODO: This will become much simpler once the LSM-BTree supports a true 'delete' operation
        // Check if the nodegroup exists already
        if (getNodeGroup(txnId, nodeGroupName) == null) {
            throw new MetadataException("Cannot drop nodegroup '" + nodeGroupName + "' because it doesn't exist");
        }

        // Check if any datasets are partitioned on this nodegroup
        List<String> datasetNames;
        try {
            datasetNames = getDatasetNamesPartitionedOnThisNodeGroup(txnId, nodeGroupName);
        } catch (Exception e) {
            throw new MetadataException(e);
        }
        if (!datasetNames.isEmpty()) {
            StringBuilder sb = new StringBuilder();
            sb.append("Nodegroup '" + nodeGroupName
                    + "' cannot be dropped; it was used for partitioning these datasets:");
            for (int i = 0; i < datasetNames.size(); i++)
                sb.append("\n" + (i + 1) + "- " + datasetNames.get(i) + ".");
            throw new MetadataException(sb.toString());
        }

        // The nodegroup exists, so drop it
        try {
            ITupleReference searchKey = createTuple(nodeGroupName);
            // Searches the index for the tuple to be deleted. Acquires an S
            // lock on the 'nodegroup' dataset.
            ITupleReference tuple = getTupleToBeDeleted(txnId, MetadataPrimaryIndexes.NODEGROUP_DATASET, searchKey);
            deleteTupleFromIndex(txnId, MetadataPrimaryIndexes.NODEGROUP_DATASET, tuple);
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public void dropDatatype(long txnId, String dataverseName, String datatypeName) throws MetadataException,
            RemoteException {
        // TODO: This will become much simpler once the LSM-BTree supports a true 'delete' operation
        // Check if the datatype exists already
        if (getDatatype(txnId, dataverseName, datatypeName) == null) {
            throw new MetadataException("Cannot drop type '" + datatypeName + "' because it doesn't exist");
        }

        List<String> datasetNames;
        List<String> usedDatatypes;
        try {
            datasetNames = getDatasetNamesDeclaredByThisDatatype(txnId, dataverseName, datatypeName);
            usedDatatypes = getDatatypeNamesUsingThisDatatype(txnId, dataverseName, datatypeName);
        } catch (Exception e) {
            throw new MetadataException(e);
        }
        // Check whether type is being used by datasets.
        if (!datasetNames.isEmpty()) {
            StringBuilder sb = new StringBuilder();
            sb.append("Cannot drop type '" + datatypeName + "'; it was used when creating these datasets:");
            for (int i = 0; i < datasetNames.size(); i++)
                sb.append("\n" + (i + 1) + "- " + datasetNames.get(i) + ".");
            throw new MetadataException(sb.toString());
        }
        // Check whether type is being used by other types.
        if (!usedDatatypes.isEmpty()) {
            StringBuilder sb = new StringBuilder();
            sb.append("Cannot drop type '" + datatypeName + "'; it is used in these datatypes:");
            for (int i = 0; i < usedDatatypes.size(); i++)
                sb.append("\n" + (i + 1) + "- " + usedDatatypes.get(i) + ".");
            throw new MetadataException(sb.toString());
        }
        // The datatype exists, so delete the datatype entry, including all it's nested types.
        try {
            ITupleReference searchKey = createTuple(dataverseName, datatypeName);
            // Searches the index for the tuple to be deleted. Acquires an S
            // lock on the 'datatype' dataset.
            ITupleReference tuple = getTupleToBeDeleted(txnId, MetadataPrimaryIndexes.DATATYPE_DATASET, searchKey);
            deleteTupleFromIndex(txnId, MetadataPrimaryIndexes.DATATYPE_DATASET, tuple);
            deleteFromDatatypeSecondaryIndex(txnId, dataverseName, datatypeName);
            List<String> nestedTypes = getNestedDatatypeNames(txnId, dataverseName, datatypeName);
            for (String nestedType : nestedTypes) {
                Datatype dt = getDatatype(txnId, dataverseName, nestedType);
                if (dt != null && dt.getIsAnonymous()) {
                    dropDatatype(txnId, dataverseName, dt.getDatatypeName());
                }
            }
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public void dropFunction(long txnId, String dataverseName, String functionName, int arity)
            throws MetadataException, RemoteException {
        // TODO: This will become much simpler once the LSM-BTree supports a true 'delete' operation
        // Check if the function exists already
        if (getFunction(txnId, dataverseName, functionName, arity) == null) {
            throw new MetadataException("Cannot drop function '" + functionName + " and arity " + arity
                    + "' because it doesn't exist.");
        }
        try {
            // Delete entry from the 'function' dataset.
            ITupleReference searchKey = createTuple(dataverseName, functionName, "" + arity);
            // Searches the index for the tuple to be deleted. Acquires an S
            // lock on the 'function' dataset.
            ITupleReference datasetTuple = getTupleToBeDeleted(txnId, MetadataPrimaryIndexes.FUNCTION_DATASET,
                    searchKey);
            deleteTupleFromIndex(txnId, MetadataPrimaryIndexes.FUNCTION_DATASET, datasetTuple);
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    private void forceDropDatatype(long txnId, String dataverseName, String datatypeName) throws AsterixException {
        try {
            ITupleReference searchKey = createTuple(dataverseName, datatypeName);
            // Searches the index for the tuple to be deleted. Acquires an S
            // lock on the 'datatype' dataset.
            ITupleReference tuple = getTupleToBeDeleted(txnId, MetadataPrimaryIndexes.DATATYPE_DATASET, searchKey);
            deleteTupleFromIndex(txnId, MetadataPrimaryIndexes.DATATYPE_DATASET, tuple);
            deleteFromDatatypeSecondaryIndex(txnId, dataverseName, datatypeName);
        } catch (AsterixException e) {
            throw e;
        } catch (Exception e) {
            throw new AsterixException(e);
        }
    }

    private void deleteFromDatatypeSecondaryIndex(long txnId, String dataverseName, String datatypeName)
            throws AsterixException {
        try {
            List<String> nestedTypes = getNestedDatatypeNames(txnId, dataverseName, datatypeName);
            for (String nestedType : nestedTypes) {
                ITupleReference searchKey = createTuple(dataverseName, nestedType, datatypeName);
                // Searches the index for the tuple to be deleted. Acquires an S
                // lock on the DATATYPENAME_ON_DATATYPE_INDEX index.
                ITupleReference tuple = getTupleToBeDeleted(txnId,
                        MetadataSecondaryIndexes.DATATYPENAME_ON_DATATYPE_INDEX, searchKey);
                deleteTupleFromIndex(txnId, MetadataSecondaryIndexes.DATATYPENAME_ON_DATATYPE_INDEX, tuple);
            }
        } catch (AsterixException e) {
            throw e;
        } catch (Exception e) {
            throw new AsterixException(e);
        }
    }

    private void deleteTupleFromIndex(long txnId, IMetadataIndex index, ITupleReference tuple) throws Exception {
        int fileId = index.getFileId();
        IIndex physicalIndex = indexRegistry.get(DataUtil.byteArrayToInt(index.getResourceId(), 0));
        physicalIndex.open(fileId);
        IIndexAccessor indexAccessor = physicalIndex.createAccessor();
        TransactionContext txnCtx = transactionProvider.getTransactionManager().getTransactionContext(txnId);
        // This lock is actually an upgrade, because a deletion must be preceded
        // by a search, in order to be able to undo an aborted deletion.
        // The transaction with txnId will have an S lock on the
        // resource. Note that lock converters have a higher priority than
        // regular waiters in the LockManager.
        transactionProvider.getLockManager().lock(txnCtx, index.getResourceId(), LockMode.EXCLUSIVE);
        indexAccessor.delete(tuple);
        index.getTreeLogger().generateLogRecord(transactionProvider, txnCtx, IndexOp.DELETE, tuple);
    }

    @Override
    public Dataverse getDataverse(long txnId, String dataverseName) throws MetadataException, RemoteException {
        try {
            ITupleReference searchKey = createTuple(dataverseName);
            DataverseTupleTranslator tupleReaderWriter = new DataverseTupleTranslator(false);
            IValueExtractor<Dataverse> valueExtractor = new MetadataEntityValueExtractor<Dataverse>(tupleReaderWriter);
            List<Dataverse> results = new ArrayList<Dataverse>();
            searchIndex(txnId, MetadataPrimaryIndexes.DATAVERSE_DATASET, searchKey, valueExtractor, results);
            if (results.isEmpty()) {
                return null;
            }
            return results.get(0);
        } catch (Exception e) {
            throw new MetadataException(e);
        }

    }

    @Override
    public List<Dataset> getDataverseDatasets(long txnId, String dataverseName) throws MetadataException,
            RemoteException {
        try {
            ITupleReference searchKey = createTuple(dataverseName);
            DatasetTupleTranslator tupleReaderWriter = new DatasetTupleTranslator(false);
            IValueExtractor<Dataset> valueExtractor = new MetadataEntityValueExtractor<Dataset>(tupleReaderWriter);
            List<Dataset> results = new ArrayList<Dataset>();
            searchIndex(txnId, MetadataPrimaryIndexes.DATASET_DATASET, searchKey, valueExtractor, results);
            return results;
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    private List<Datatype> getDataverseDatatypes(long txnId, String dataverseName) throws MetadataException,
            RemoteException {
        try {
            ITupleReference searchKey = createTuple(dataverseName);
            DatatypeTupleTranslator tupleReaderWriter = new DatatypeTupleTranslator(txnId, this, false);
            IValueExtractor<Datatype> valueExtractor = new MetadataEntityValueExtractor<Datatype>(tupleReaderWriter);
            List<Datatype> results = new ArrayList<Datatype>();
            searchIndex(txnId, MetadataPrimaryIndexes.DATATYPE_DATASET, searchKey, valueExtractor, results);
            return results;
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public Dataset getDataset(long txnId, String dataverseName, String datasetName) throws MetadataException,
            RemoteException {
        try {
            ITupleReference searchKey = createTuple(dataverseName, datasetName);
            DatasetTupleTranslator tupleReaderWriter = new DatasetTupleTranslator(false);
            List<Dataset> results = new ArrayList<Dataset>();
            IValueExtractor<Dataset> valueExtractor = new MetadataEntityValueExtractor<Dataset>(tupleReaderWriter);
            searchIndex(txnId, MetadataPrimaryIndexes.DATASET_DATASET, searchKey, valueExtractor, results);
            if (results.isEmpty()) {
                return null;
            }
            return results.get(0);
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    private List<String> getDatasetNamesDeclaredByThisDatatype(long txnId, String dataverseName, String datatypeName)
            throws MetadataException, RemoteException {
        try {
            ITupleReference searchKey = createTuple(dataverseName, dataverseName);
            List<String> results = new ArrayList<String>();
            IValueExtractor<String> valueExtractor = new DatasetNameValueExtractor();
            searchIndex(txnId, MetadataSecondaryIndexes.DATATYPENAME_ON_DATASET_INDEX, searchKey, valueExtractor,
                    results);
            return results;
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    public List<String> getDatatypeNamesUsingThisDatatype(long txnId, String dataverseName, String datatypeName)
            throws MetadataException, RemoteException {
        try {
            ITupleReference searchKey = createTuple(dataverseName, datatypeName);
            List<String> results = new ArrayList<String>();
            IValueExtractor<String> valueExtractor = new DatatypeNameValueExtractor(dataverseName, this);
            searchIndex(txnId, MetadataSecondaryIndexes.DATATYPENAME_ON_DATATYPE_INDEX, searchKey, valueExtractor,
                    results);
            return results;
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    private List<String> getNestedDatatypeNames(long txnId, String dataverseName, String datatypeName)
            throws MetadataException, RemoteException {
        try {
            ITupleReference searchKey = createTuple(dataverseName);
            List<String> results = new ArrayList<String>();
            IValueExtractor<String> valueExtractor = new NestedDatatypeNameValueExtractor(datatypeName);
            searchIndex(txnId, MetadataSecondaryIndexes.DATATYPENAME_ON_DATATYPE_INDEX, searchKey, valueExtractor,
                    results);
            return results;
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    public List<String> getDatasetNamesPartitionedOnThisNodeGroup(long txnId, String nodegroup)
            throws MetadataException, RemoteException {
        try {
            ITupleReference searchKey = createTuple(nodegroup);
            List<String> results = new ArrayList<String>();
            IValueExtractor<String> valueExtractor = new DatasetNameValueExtractor();
            searchIndex(txnId, MetadataSecondaryIndexes.GROUPNAME_ON_DATASET_INDEX, searchKey, valueExtractor, results);
            return results;
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public Index getIndex(long txnId, String dataverseName, String datasetName, String indexName)
            throws MetadataException, RemoteException {
        try {
            ITupleReference searchKey = createTuple(dataverseName, datasetName, indexName);
            IndexTupleTranslator tupleReaderWriter = new IndexTupleTranslator(false);
            IValueExtractor<Index> valueExtractor = new MetadataEntityValueExtractor<Index>(tupleReaderWriter);
            List<Index> results = new ArrayList<Index>();
            searchIndex(txnId, MetadataPrimaryIndexes.INDEX_DATASET, searchKey, valueExtractor, results);
            if (results.isEmpty()) {
                return null;
            }
            return results.get(0);
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public List<Index> getDatasetIndexes(long txnId, String dataverseName, String datasetName)
            throws MetadataException, RemoteException {
        try {
            ITupleReference searchKey = createTuple(dataverseName, datasetName);
            IndexTupleTranslator tupleReaderWriter = new IndexTupleTranslator(false);
            IValueExtractor<Index> valueExtractor = new MetadataEntityValueExtractor<Index>(tupleReaderWriter);
            List<Index> results = new ArrayList<Index>();
            searchIndex(txnId, MetadataPrimaryIndexes.INDEX_DATASET, searchKey, valueExtractor, results);
            return results;
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public Datatype getDatatype(long txnId, String dataverseName, String datatypeName) throws MetadataException,
            RemoteException {
        try {
            ITupleReference searchKey = createTuple(dataverseName, datatypeName);
            DatatypeTupleTranslator tupleReaderWriter = new DatatypeTupleTranslator(txnId, this, false);
            IValueExtractor<Datatype> valueExtractor = new MetadataEntityValueExtractor<Datatype>(tupleReaderWriter);
            List<Datatype> results = new ArrayList<Datatype>();
            searchIndex(txnId, MetadataPrimaryIndexes.DATATYPE_DATASET, searchKey, valueExtractor, results);
            if (results.isEmpty()) {
                return null;
            }
            return results.get(0);
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    private Node getNode(long txnId, String nodeName) throws MetadataException {
        try {
            ITupleReference searchKey = createTuple(nodeName);
            NodeTupleTranslator tupleReaderWriter = new NodeTupleTranslator(false);
            IValueExtractor<Node> valueExtractor = new MetadataEntityValueExtractor<Node>(tupleReaderWriter);
            List<Node> results = new ArrayList<Node>();
            searchIndex(txnId, MetadataPrimaryIndexes.NODE_DATASET, searchKey, valueExtractor, results);
            if (results.isEmpty()) {
                return null;
            }
            return results.get(0);
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public NodeGroup getNodeGroup(long txnId, String nodeGroupName) throws MetadataException, RemoteException {
        try {
            ITupleReference searchKey = createTuple(nodeGroupName);
            NodeGroupTupleTranslator tupleReaderWriter = new NodeGroupTupleTranslator(false);
            IValueExtractor<NodeGroup> valueExtractor = new MetadataEntityValueExtractor<NodeGroup>(tupleReaderWriter);
            List<NodeGroup> results = new ArrayList<NodeGroup>();
            searchIndex(txnId, MetadataPrimaryIndexes.NODEGROUP_DATASET, searchKey, valueExtractor, results);
            if (results.isEmpty()) {
                return null;
            }
            return results.get(0);
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    @Override
    public Function getFunction(long txnId, String dataverseName, String functionName, int arity)
            throws MetadataException, RemoteException {
        try {
            ITupleReference searchKey = createTuple(dataverseName, functionName, "" + arity);
            FunctionTupleTranslator tupleReaderWriter = new FunctionTupleTranslator(false);
            List<Function> results = new ArrayList<Function>();
            IValueExtractor<Function> valueExtractor = new MetadataEntityValueExtractor<Function>(tupleReaderWriter);
            searchIndex(txnId, MetadataPrimaryIndexes.FUNCTION_DATASET, searchKey, valueExtractor, results);
            if (results.isEmpty()) {
                return null;
            }
            return results.get(0);
        } catch (Exception e) {
            throw new MetadataException(e);
        }
    }

    private ITupleReference getTupleToBeDeleted(long txnId, IMetadataIndex metadataIndex, ITupleReference searchKey)
            throws Exception {
        IValueExtractor<ITupleReference> valueExtractor = new TupleCopyValueExtractor(metadataIndex.getTypeTraits());
        List<ITupleReference> results = new ArrayList<ITupleReference>();
        searchIndex(txnId, metadataIndex, searchKey, valueExtractor, results);
        if (results.isEmpty()) {
            throw new MetadataException("Could not find entry to be deleted.");
        }
        // There should be exactly one result returned from the search.
        return results.get(0);
    }

    private <ResultType> void searchIndex(long txnId, IMetadataIndex index, ITupleReference searchKey,
            IValueExtractor<ResultType> valueExtractor, List<ResultType> results) throws Exception {
        TransactionContext txnCtx = transactionProvider.getTransactionManager().getTransactionContext(txnId);
        transactionProvider.getLockManager().lock(txnCtx, index.getResourceId(), LockMode.SHARED);

        int fileId = index.getFileId();
        IIndex physicalIndex = indexRegistry.get(DataUtil.byteArrayToInt(index.getResourceId(), 0));
        physicalIndex.open(fileId);
        IIndexAccessor indexAccessor = physicalIndex.createAccessor();
        IIndexCursor rangeCursor = indexAccessor.createSearchCursor();

        IBinaryComparatorFactory[] comparatorFactories = index.getKeyBinaryComparatorFactory();
        IBinaryComparator[] searchCmps = new IBinaryComparator[searchKey.getFieldCount()];
        for (int i = 0; i < searchKey.getFieldCount(); i++) {
            searchCmps[i] = comparatorFactories[i].createBinaryComparator();
        }
        MultiComparator searchCmp = new MultiComparator(searchCmps);
        RangePredicate rangePred = new RangePredicate(searchKey, searchKey, true, true, searchCmp, searchCmp);

        indexAccessor.search(rangeCursor, rangePred);
        try {
            while (rangeCursor.hasNext()) {
                rangeCursor.next();
                ResultType result = valueExtractor.getValue(txnId, rangeCursor.getTuple());
                if (result != null) {
                    results.add(result);
                }
            }
        } finally {
            rangeCursor.close();
        }
    }

    public ITupleReference createTuple(String... fields) throws HyracksDataException {
        ISerializerDeserializer[] fieldSerdes = new ISerializerDeserializer[fields.length];
        for (int i = 0; i < fields.length; i++) {
            fieldSerdes[i] = UTF8StringSerializerDeserializer.INSTANCE;
        }
        return TupleUtils.createTuple(fieldSerdes, (Object[]) fields);
    }

    /**
     * Creates resourceIdSeed(which is an AtomicInteger instance) and
     * initialize the value with the given initialValue.
     * 
     * @param initialValue
     *            resourceIdSeed is set to this initialValue.
     */
    @Override
    public void createResourceIdSeed(int initialValue) throws RemoteException{
        resourceIdSeed.set(initialValue);
    }

    /**
     * @return a uniquely generated resourceID.
     */
    @Override
    public int generateResourceId() throws RemoteException{
        return resourceIdSeed.getAndIncrement();
    }

    @Override
    public int getResourceId(long txnId, String dataverseName, String datasetName, String indexName)
            throws MetadataException, RemoteException {
        return getIndex(txnId, dataverseName, datasetName, indexName).getResourceId();
    }

    /**
     * @return the maximum resourceID of the resourceIDs which have been generated so far
     */
    @Override
    public int getGeneratedMaxResourceId() throws RemoteException, Exception {
        int maxResourceId = -1;
        IMetadataIndex metadataIndex = MetadataPrimaryIndexes.INDEX_DATASET;

        //Prepare IndexTupleTranslator
        IndexTupleTranslator tupleReaderWriter = new IndexTupleTranslator(false);
        IValueExtractor<Index> valueExtractor = new MetadataEntityValueExtractor<Index>(tupleReaderWriter);

        //get the index from indexRegistry using resourceId
        IIndex physicalIndex = indexRegistry.get(DataUtil.byteArrayToInt(metadataIndex.getResourceId(), 0));
        int fileId = metadataIndex.getFileId();
        physicalIndex.open(fileId);

        //create a rangePredicate(which is null predicate) and create a cursor with the predicate
        IIndexAccessor indexAccessor = physicalIndex.createAccessor();
        IIndexCursor rangeCursor = indexAccessor.createSearchCursor();
        RangePredicate rangePred = new RangePredicate(null, null, true, true, null, null);
        indexAccessor.search(rangeCursor, rangePred);

        //while iterating each record(which includes an index information), keep the maxResourceId.
        try {
            while (rangeCursor.hasNext()) {
                rangeCursor.next();
                //TODO handle the issue of the transactionId or transactionContext during recovery 
                Index index = valueExtractor.getValue(0, rangeCursor.getTuple());
                if (index.getResourceId() > maxResourceId) {
                    maxResourceId = index.getResourceId();
                }
            }
        } finally {
            rangeCursor.close();
        }

        return maxResourceId;

    }
}

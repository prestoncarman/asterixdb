package edu.uci.ics.asterix.algebra.operators.physical;

import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.asterix.common.config.DatasetConfig.DatasetType;
import edu.uci.ics.asterix.common.functions.FunctionArgumentsConstants;
import edu.uci.ics.asterix.dataflow.base.IAsterixApplicationContextInfo;
import edu.uci.ics.asterix.formats.nontagged.AqlBinaryComparatorFactoryProvider;
import edu.uci.ics.asterix.formats.nontagged.AqlTypeTraitProvider;
import edu.uci.ics.asterix.metadata.MetadataException;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledDatasetDecl;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledIndexDecl;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledMetadataDeclarations;
import edu.uci.ics.asterix.metadata.declared.AqlMetadataProvider;
import edu.uci.ics.asterix.metadata.declared.AqlSourceId;
import edu.uci.ics.asterix.metadata.utils.DatasetUtils;
import edu.uci.ics.asterix.om.base.IAObject;
import edu.uci.ics.asterix.om.constants.AsterixConstantValue;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.optimizer.rules.am.InvertedIndexAccessMethod;
import edu.uci.ics.asterix.optimizer.rules.am.InvertedIndexAccessMethod.SearchModifierType;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IDataSourceIndex;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.OrderOperator.IOrder.OrderKind;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.base.IEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.jobgen.impl.JobGenContext;
import edu.uci.ics.hyracks.algebricks.core.api.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.NotImplementedException;
import edu.uci.ics.hyracks.algebricks.core.utils.Pair;
import edu.uci.ics.hyracks.algebricks.core.utils.Triple;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.storage.am.btree.dataflow.BTreeDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedIndexSearchModifierFactory;
import edu.uci.ics.hyracks.storage.am.invertedindex.dataflow.InvertedIndexSearchOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.invertedindex.tokenizers.IBinaryTokenizerFactory;

public class InvertedIndexPOperator extends IndexSearchPOperator {
    public InvertedIndexPOperator(IDataSourceIndex<String, AqlSourceId> idx) {
        super(idx);
    }

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        // TODO: Currently, I'm relying on my own version of Algebricks, not the released one.
        // Need to add this tag in Algebricks.
        return PhysicalOperatorTag.INVERTED_INDEX_SEARCH;
    }

    @Override
    public void contributeRuntimeOperator(IHyracksJobBuilder builder, JobGenContext context, ILogicalOperator op,
            IOperatorSchema opSchema, IOperatorSchema[] inputSchemas, IOperatorSchema outerPlanSchema)
                    throws AlgebricksException {
        UnnestMapOperator unnestMapOp = (UnnestMapOperator) op;
        ILogicalExpression unnestExpr = unnestMapOp.getExpressionRef().getValue();
        if (unnestExpr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            throw new IllegalStateException();
        }
        AbstractFunctionCallExpression unnestFuncExpr = (AbstractFunctionCallExpression) unnestExpr;
        if (unnestFuncExpr.getFunctionIdentifier() != AsterixBuiltinFunctions.INDEX_SEARCH) {
            return;
        }
        contributeInvertedIndexSearch(builder, context, unnestMapOp, opSchema, inputSchemas);
    }

    private void contributeInvertedIndexSearch(IHyracksJobBuilder builder, JobGenContext context, UnnestMapOperator unnestMap,
            IOperatorSchema opSchema, IOperatorSchema[] inputSchemas) throws AlgebricksException, AlgebricksException {
        Mutable<ILogicalExpression> unnestExpr = unnestMap.getExpressionRef();
        AbstractFunctionCallExpression unnestFuncExpr = (AbstractFunctionCallExpression) unnestExpr.getValue();
        // Get name of secondary index to be used.
        String indexName = getStringArgument(unnestFuncExpr, 0);
        // Get type of index and do sanity check.
        String indexType = getStringArgument(unnestFuncExpr, 1);
        if (!indexType.equals(FunctionArgumentsConstants.WORD_INVERTED_INDEX) && !indexType.equals(FunctionArgumentsConstants.NGRAM_INVERTED_INDEX)) {
            throw new NotImplementedException(indexType + " indexes are not implemented.");
        }
        // Get dataset, and do sanity check.
        String datasetName = getStringArgument(unnestFuncExpr, 2);
        AqlMetadataProvider metadataProvider = (AqlMetadataProvider) context.getMetadataProvider();
        AqlCompiledMetadataDeclarations metadata = metadataProvider.getMetadataDeclarations();
        AqlCompiledDatasetDecl datasetDecl = metadata.findDataset(datasetName);
        if (datasetDecl == null) {
            throw new AlgebricksException("Unknown dataset " + datasetName);
        }
        if (datasetDecl.getDatasetType() == DatasetType.EXTERNAL) {
            throw new AlgebricksException("Trying to run inverted index search over external dataset (" + datasetName + ").");
        }
        // Get search modifier type.
        int searchModifierOrdinal = getInt32Argument(unnestFuncExpr, 3);
        SearchModifierType searchModifierType = SearchModifierType.values()[searchModifierOrdinal];
        // Similarity threshold. Concrete type depends on search modifier.
        IAObject simThresh = ((AsterixConstantValue) ((ConstantExpression) unnestFuncExpr.getArguments().get(4).getValue())
                .getValue()).getObject();
        // Get type of search key.
        int typeTagOrdinal = getInt32Argument(unnestFuncExpr, 5);
        ATypeTag searchKeyType = ATypeTag.values()[typeTagOrdinal];
        Pair<int[], Integer> keys = getKeys(unnestFuncExpr, 6, inputSchemas);
        
        // Build runtime.
        Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> invIndexSearch = buildInvertedIndexRuntime(
                metadata, context, builder.getJobSpec(), datasetName, datasetDecl, indexName, searchKeyType, keys.first, searchModifierType, simThresh);
        // Contribute operator in hyracks job.
        builder.contributeHyracksOperator(unnestMap, invIndexSearch.first);
        builder.contributeAlgebricksPartitionConstraint(invIndexSearch.first, invIndexSearch.second);
        ILogicalOperator srcExchange = unnestMap.getInputs().get(0).getValue();
        builder.contributeGraphEdge(srcExchange, 0, unnestMap, 0);
    }
    
    @SuppressWarnings("rawtypes")
    public static Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> buildInvertedIndexRuntime(
            AqlCompiledMetadataDeclarations metadata, JobGenContext context,
            JobSpecification jobSpec, String datasetName,
            AqlCompiledDatasetDecl datasetDecl, String indexName,
            ATypeTag searchKeyType, int[] keyFields, SearchModifierType searchModifierType,
            IAObject simThresh) throws AlgebricksException {
        String itemTypeName = datasetDecl.getItemTypeName();
        IAType itemType;
        try {
            itemType = metadata.findType(itemTypeName);
        } catch (Exception e) {
            throw new AlgebricksException(e);
        }

        int numPrimaryKeys = DatasetUtils.getPartitioningFunctions(datasetDecl).size();
        AqlCompiledIndexDecl index = DatasetUtils.findSecondaryIndexByName(datasetDecl, indexName);
        if (index == null) {
            throw new AlgebricksException("Code generation error: no index " + indexName + " for dataset "
                    + datasetName);
        }
        List<String> secondaryKeyFields = index.getFieldExprs();
        int numSecondaryKeys = secondaryKeyFields.size();
        if (numSecondaryKeys != 1) {
            throw new AlgebricksException(
                    "Cannot use "
                            + numSecondaryKeys
                            + " fields as a key for the R-tree index. There can be only one field as a key for the R-tree index.");
        }
        if (itemType.getTypeTag() != ATypeTag.RECORD) {
            throw new AlgebricksException("Only record types can be indexed.");
        }
        ARecordType recordType = (ARecordType) itemType;
        IAType secondaryKeyType = AqlCompiledIndexDecl.keyFieldType(secondaryKeyFields.get(0), recordType);
        if (secondaryKeyType == null) {
            throw new AlgebricksException("Could not find field " + secondaryKeyFields.get(0) + " in the schema.");
        }

        // TODO: For now we assume the type of the generated tokens is the same as the indexed field.
        // We need a better way of expressing this because tokens may be hashed, or an inverted-index may index a list type, etc.
        ITypeTraits[] tokenTypeTraits = new ITypeTraits[numSecondaryKeys];
        IBinaryComparatorFactory[] tokenComparatorFactories = new IBinaryComparatorFactory[numSecondaryKeys];
        for (int i = 0; i < numSecondaryKeys; i++) {   
            tokenComparatorFactories[i] = InvertedIndexAccessMethod.getTokenBinaryComparatorFactory(secondaryKeyType);
            tokenTypeTraits[i] = InvertedIndexAccessMethod.getTokenTypeTrait(secondaryKeyType);
        }
        
        // The inverted lists contain primary keys.
        ISerializerDeserializer[] invListsRecFields = new ISerializerDeserializer[numPrimaryKeys];
        ITypeTraits[] invListsTypeTraits = new ITypeTraits[numPrimaryKeys];
        IBinaryComparatorFactory[] invListsComparatorFactories = new IBinaryComparatorFactory[numPrimaryKeys];
        int i = 0;
        for (Triple<IEvaluatorFactory, ScalarFunctionCallExpression, IAType> evalFactoryAndType : DatasetUtils
                .getPartitioningFunctions(datasetDecl)) {
            IAType keyType = evalFactoryAndType.third;
            ISerializerDeserializer keySerde = metadata.getFormat().getSerdeProvider()
                    .getSerializerDeserializer(keyType);
            invListsRecFields[i] = keySerde;
            invListsComparatorFactories[i] = AqlBinaryComparatorFactoryProvider.INSTANCE.getBinaryComparatorFactory(
                    keyType, OrderKind.ASC);
            invListsTypeTraits[i] = AqlTypeTraitProvider.INSTANCE.getTypeTrait(keyType);
            ++i;
        }        
        RecordDescriptor invListRecDesc = new RecordDescriptor(invListsRecFields);
        
        IAsterixApplicationContextInfo appContext = (IAsterixApplicationContextInfo) context.getAppContext();        
        Pair<IFileSplitProvider, AlgebricksPartitionConstraint> secondarySplitsAndConstraint;
        try {
            secondarySplitsAndConstraint = metadata
                    .splitProviderAndPartitionConstraintsForInternalOrFeedDataset(
                            datasetName, indexName);
        } catch (MetadataException e) {
            throw new AlgebricksException(e);
        }
        
        Pair<IFileSplitProvider, IFileSplitProvider> fileSplitProviders = metadata
                .getInvertedIndexFileSplitProviders(secondarySplitsAndConstraint.first);
        
        // TODO: Here we assume there is only one search key field.
        int queryField = keyFields[0];
        // Get tokenizer and search modifier factories.
        // TODO: retainInput is hardcoded to false, but should be passed be the caller.
        IInvertedIndexSearchModifierFactory searchModifierFactory = InvertedIndexAccessMethod.getSearchModifierFactory(searchModifierType, simThresh, index);
        IBinaryTokenizerFactory queryTokenizerFactory = InvertedIndexAccessMethod.getBinaryTokenizerFactory(searchModifierType, searchKeyType, index);
        InvertedIndexSearchOperatorDescriptor invIndexSearchOp = new InvertedIndexSearchOperatorDescriptor(
                jobSpec, queryField, appContext.getStorageManagerInterface(),
                fileSplitProviders.first, fileSplitProviders.second,
                appContext.getIndexRegistryProvider(), tokenTypeTraits,
                tokenComparatorFactories, invListsTypeTraits,
                invListsComparatorFactories, new BTreeDataflowHelperFactory(),
                queryTokenizerFactory, searchModifierFactory, invListRecDesc, false);
        return new Pair<IOperatorDescriptor, AlgebricksPartitionConstraint>(invIndexSearchOp, secondarySplitsAndConstraint.second);
    }
}

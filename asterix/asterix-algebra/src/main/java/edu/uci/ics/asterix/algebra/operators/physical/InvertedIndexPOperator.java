package edu.uci.ics.asterix.algebra.operators.physical;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.asterix.common.config.DatasetConfig.DatasetType;
import edu.uci.ics.asterix.common.functions.FunctionArgumentsConstants;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledDatasetDecl;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledMetadataDeclarations;
import edu.uci.ics.asterix.metadata.declared.AqlMetadataProvider;
import edu.uci.ics.asterix.metadata.declared.AqlSourceId;
import edu.uci.ics.asterix.om.base.AInt32;
import edu.uci.ics.asterix.om.base.IAObject;
import edu.uci.ics.asterix.om.constants.AsterixConstantValue;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IDataSourceIndex;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractScanOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.jobgen.impl.JobGenContext;
import edu.uci.ics.hyracks.algebricks.core.api.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.NotImplementedException;
import edu.uci.ics.hyracks.algebricks.core.utils.Pair;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;

public class InvertedIndexPOperator extends IndexSearchPOperator {
    public InvertedIndexPOperator(IDataSourceIndex<String, AqlSourceId> idx) {
        super(idx);
    }

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        // TODO: Currently, I'm relying on my own version of Algebricks, not the released one.
        // Need to add this tag in Algebricks.
        return PhysicalOperatorTag.INVERED_INDEX_SEARCH;
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
        FunctionIdentifier funcIdent = unnestFuncExpr.getFunctionIdentifier();
        if (funcIdent.equals(AsterixBuiltinFunctions.INDEX_SEARCH)) {
            contributeInvertedIndexSearch(builder, context, unnestMapOp, opSchema, inputSchemas);
            return;
        }
    }

    private void contributeInvertedIndexSearch(IHyracksJobBuilder builder, JobGenContext context, UnnestMapOperator unnestMap,
            IOperatorSchema opSchema, IOperatorSchema[] inputSchemas) throws AlgebricksException, AlgebricksException {
        Mutable<ILogicalExpression> unnestExpr = unnestMap.getExpressionRef();
        AbstractFunctionCallExpression unnestFuncExpr = (AbstractFunctionCallExpression) unnestExpr.getValue();
        String indexType = getStringArgument(unnestFuncExpr, 1);
        if (!indexType.equals(FunctionArgumentsConstants.WORD_INVERTED_INDEX) && !indexType.equals(FunctionArgumentsConstants.NGRAM_INVERTED_INDEX)) {
            throw new NotImplementedException(indexType + " indexes are not implemented.");
        }
        String indexName = getStringArgument(unnestFuncExpr, 0);
        String datasetName = getStringArgument(unnestFuncExpr, 2);
        String searchModifierName = getStringArgument(unnestFuncExpr, 3);
        // Similarity threshold. Concrete type depends on search modifier.
        IAObject simThresh = ((AsterixConstantValue) ((ConstantExpression) unnestFuncExpr.getArguments().get(4).getValue())
                .getValue()).getObject();
        // Get type of search key.
        IAObject typeTagObj = ((AsterixConstantValue) ((ConstantExpression) unnestFuncExpr.getArguments().get(5).getValue())
                .getValue()).getObject();
        ATypeTag searchKeyType = ATypeTag.values()[((AInt32)typeTagObj).getIntegerValue()];
        Pair<int[], Integer> keys = getKeys(unnestFuncExpr, 6, inputSchemas);
        buildInvertedIndexSearch(builder, context, unnestMap, opSchema, datasetName, indexName, searchKeyType, keys.first, searchModifierName, simThresh);
    }

    private static void buildInvertedIndexSearch(IHyracksJobBuilder builder, JobGenContext context, AbstractScanOperator scan,
            IOperatorSchema opSchema, String datasetName, String indexName, ATypeTag searchKeyType, int[] keyFields, String searchModifierName, IAObject simThresh)
            throws AlgebricksException, AlgebricksException {
        AqlMetadataProvider metadataProvider = (AqlMetadataProvider) context.getMetadataProvider();
        AqlCompiledMetadataDeclarations metadata = metadataProvider.getMetadataDeclarations();
        AqlCompiledDatasetDecl datasetDecl = metadata.findDataset(datasetName);
        if (datasetDecl == null) {
            throw new AlgebricksException("Unknown dataset " + datasetName);
        }
        if (datasetDecl.getDatasetType() == DatasetType.EXTERNAL) {
            throw new AlgebricksException("Trying to run inverted index search over external dataset (" + datasetName + ").");
        }
        Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> invIndexSearch = AqlMetadataProvider.buildInvertedIndexRuntime(
                metadata, context, builder.getJobSpec(), datasetName, datasetDecl, indexName, searchKeyType, keyFields, searchModifierName, simThresh);
        builder.contributeHyracksOperator(scan, invIndexSearch.first);
        builder.contributeAlgebricksPartitionConstraint(invIndexSearch.first, invIndexSearch.second);
        ILogicalOperator srcExchange = scan.getInputs().get(0).getValue();
        builder.contributeGraphEdge(srcExchange, 0, scan, 0);
    }
}

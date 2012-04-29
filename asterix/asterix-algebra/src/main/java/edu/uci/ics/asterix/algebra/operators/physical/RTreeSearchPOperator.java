package edu.uci.ics.asterix.algebra.operators.physical;


import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.asterix.common.config.DatasetConfig.DatasetType;
import edu.uci.ics.asterix.common.functions.FunctionArgumentsConstants;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledDatasetDecl;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledMetadataDeclarations;
import edu.uci.ics.asterix.metadata.declared.AqlMetadataProvider;
import edu.uci.ics.asterix.metadata.declared.AqlSourceId;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.optimizer.rules.am.AccessMethodUtils;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
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

public class RTreeSearchPOperator extends IndexSearchPOperator {

    public RTreeSearchPOperator(IDataSourceIndex<String, AqlSourceId> idx, boolean requiresBroadcast) {
        super(idx, requiresBroadcast);
    }

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.RTREE_SEARCH;
    }

    @Override
    public void contributeRuntimeOperator(IHyracksJobBuilder builder, JobGenContext context, ILogicalOperator op,
            IOperatorSchema opSchema, IOperatorSchema[] inputSchemas, IOperatorSchema outerPlanSchema)
            throws AlgebricksException {
        UnnestMapOperator unnestMap = (UnnestMapOperator) op;
        ILogicalExpression unnestExpr = unnestMap.getExpressionRef().getValue();

        if (unnestExpr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
            AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) unnestExpr;
            FunctionIdentifier fid = f.getFunctionIdentifier();
            if (fid.equals(AsterixBuiltinFunctions.INDEX_SEARCH)) {
                try {
                    contributeRtreeSearch(builder, context, unnestMap, opSchema, inputSchemas);
                } catch (AlgebricksException e) {
                    throw new AlgebricksException(e);
                }
                return;
            }
        }
        throw new IllegalStateException();
    }

    private void contributeRtreeSearch(IHyracksJobBuilder builder, JobGenContext context, UnnestMapOperator unnestMap,
            IOperatorSchema opSchema, IOperatorSchema[] inputSchemas) throws AlgebricksException, AlgebricksException {
        Mutable<ILogicalExpression> unnestExpr = unnestMap.getExpressionRef();
        AbstractFunctionCallExpression unnestFuncExpr = (AbstractFunctionCallExpression) unnestExpr.getValue();

        String idxType = AccessMethodUtils.getStringConstant(unnestFuncExpr.getArguments().get(1));
        if (idxType != FunctionArgumentsConstants.RTREE_INDEX) {
            throw new NotImplementedException(idxType + " indexes are not implemented.");
        }
        String idxName = AccessMethodUtils.getStringConstant(unnestFuncExpr.getArguments().get(0));
        String datasetName = AccessMethodUtils.getStringConstant(unnestFuncExpr.getArguments().get(2));
        boolean retainInput = AccessMethodUtils.getBooleanConstant(unnestFuncExpr.getArguments().get(3));
        boolean requiresBroadcast = AccessMethodUtils.getBooleanConstant(unnestFuncExpr.getArguments().get(4));

        Pair<int[], Integer> keys = getKeys(unnestFuncExpr, 5, inputSchemas);
        buildRtreeSearch(builder, context, unnestMap, opSchema, datasetName, idxName, keys.first);
    }

    private static void buildRtreeSearch(IHyracksJobBuilder builder, JobGenContext context, AbstractScanOperator scan,
            IOperatorSchema opSchema, String datasetName, String indexName, int[] keyFields)
            throws AlgebricksException, AlgebricksException {
        AqlMetadataProvider mp = (AqlMetadataProvider) context.getMetadataProvider();
        AqlCompiledMetadataDeclarations metadata = mp.getMetadataDeclarations();
        AqlCompiledDatasetDecl adecl = metadata.findDataset(datasetName);
        if (adecl == null) {
            throw new AlgebricksException("Unknown dataset " + datasetName);
        }
        if (adecl.getDatasetType() == DatasetType.EXTERNAL) {
            throw new AlgebricksException("Trying to run rtree search over external dataset (" + datasetName + ").");
        }
        Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> rtreeSearch = AqlMetadataProvider.buildRtreeRuntime(
                metadata, context, builder.getJobSpec(), datasetName, adecl, indexName, keyFields);
        builder.contributeHyracksOperator(scan, rtreeSearch.first);
        builder.contributeAlgebricksPartitionConstraint(rtreeSearch.first, rtreeSearch.second);

        ILogicalOperator srcExchange = scan.getInputs().get(0).getValue();
        builder.contributeGraphEdge(srcExchange, 0, scan, 0);
    }

}

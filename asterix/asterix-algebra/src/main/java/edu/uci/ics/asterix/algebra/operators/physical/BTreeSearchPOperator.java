package edu.uci.ics.asterix.algebra.operators.physical;


import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.asterix.common.config.DatasetConfig.DatasetType;
import edu.uci.ics.asterix.common.functions.FunctionArgumentsConstants;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledDatasetDecl;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledMetadataDeclarations;
import edu.uci.ics.asterix.metadata.declared.AqlMetadataProvider;
import edu.uci.ics.asterix.metadata.declared.AqlSourceId;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IDataSourceIndex;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.jobgen.impl.JobGenContext;
import edu.uci.ics.hyracks.algebricks.core.api.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.NotImplementedException;
import edu.uci.ics.hyracks.algebricks.core.utils.Pair;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;

public class BTreeSearchPOperator extends IndexSearchPOperator {

    public BTreeSearchPOperator(IDataSourceIndex<String, AqlSourceId> idx, boolean requiresBroadcast) {
        super(idx, requiresBroadcast);
    }

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.BTREE_SEARCH;
    }

    @Override
    public void contributeRuntimeOperator(IHyracksJobBuilder builder, JobGenContext context, ILogicalOperator op,
            IOperatorSchema opSchema, IOperatorSchema[] inputSchemas, IOperatorSchema outerPlanSchema)
                    throws AlgebricksException {
        UnnestMapOperator unnestMap = (UnnestMapOperator) op;
        ILogicalExpression unnestExpr = unnestMap.getExpressionRef().getValue();
        if (unnestExpr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            throw new IllegalStateException();
        }
        AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) unnestExpr;
        FunctionIdentifier fid = f.getFunctionIdentifier();
        if (fid.equals(AsterixBuiltinFunctions.INDEX_SEARCH)) {
            contributeBtreeSearch(builder, context, unnestMap, opSchema, inputSchemas);
            return;
        }
    }

    private void contributeBtreeSearch(IHyracksJobBuilder builder, JobGenContext context, UnnestMapOperator unnestMap,
            IOperatorSchema opSchema, IOperatorSchema[] inputSchemas) throws AlgebricksException, AlgebricksException {
        Mutable<ILogicalExpression> unnestExpr = unnestMap.getExpressionRef();
        AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) unnestExpr.getValue();

        String idxType = getStringArgument(f, 1);
        if (idxType != FunctionArgumentsConstants.BTREE_INDEX) {
            throw new NotImplementedException(idxType + " indexes are not implemented.");
        }
        String idxName = getStringArgument(f, 0);
        String datasetName = getStringArgument(f, 2);
        boolean retainInput = getBooleanArgument(f, 3);
        boolean requiresBroadcast = getBooleanArgument(f, 4);

        Pair<int[], Integer> keysLeft = getKeys(f, 5, inputSchemas);
        Pair<int[], Integer> keysRight = getKeys(f, 6 + keysLeft.second, inputSchemas);

        int p = 7 + keysLeft.second + keysRight.second;
        boolean loInclusive = isTrue((ConstantExpression) f.getArguments().get(p).getValue());
        boolean hiInclusive = isTrue((ConstantExpression) f.getArguments().get(p + 1).getValue());
        
        AqlMetadataProvider mp = (AqlMetadataProvider) context.getMetadataProvider();
        AqlCompiledMetadataDeclarations metadata = mp.getMetadataDeclarations();
        AqlCompiledDatasetDecl adecl = metadata.findDataset(datasetName);
        if (adecl == null) {
            throw new AlgebricksException("Unknown dataset " + datasetName);
        }
        if (adecl.getDatasetType() == DatasetType.EXTERNAL) {
            throw new AlgebricksException("Trying to run btree search over external dataset (" + datasetName + ").");
        }
        IVariableTypeEnvironment typeEnv = context.getTypeEnvironment(unnestMap);
        Pair<IOperatorDescriptor, AlgebricksPartitionConstraint> btreeSearch = AqlMetadataProvider.buildBtreeRuntime(
                metadata, context, builder.getJobSpec(), typeEnv, inputSchemas, retainInput, datasetName, adecl, idxName, keysLeft.first, keysRight.first,
                loInclusive, hiInclusive);
        builder.contributeHyracksOperator(unnestMap, btreeSearch.first);
        builder.contributeAlgebricksPartitionConstraint(btreeSearch.first, btreeSearch.second);

        ILogicalOperator srcExchange = unnestMap.getInputs().get(0).getValue();
        builder.contributeGraphEdge(srcExchange, 0, unnestMap, 0);
    }

    private boolean isTrue(ConstantExpression k) {
        return k.getValue().isTrue();
    }
}
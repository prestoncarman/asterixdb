package edu.uci.ics.asterix.optimizer.rules.am;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import edu.uci.ics.asterix.common.functions.FunctionArgumentsConstants;
import edu.uci.ics.asterix.common.functions.FunctionUtils;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledDatasetDecl;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledIndexDecl;
import edu.uci.ics.asterix.metadata.utils.DatasetUtils;
import edu.uci.ics.asterix.om.base.AInt32;
import edu.uci.ics.asterix.om.base.AString;
import edu.uci.ics.asterix.om.constants.AsterixConstantValue;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IAlgebricksConstantValue;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.UnnestingFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator.ExecutionMode;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.OrderOperator.IOrder;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.base.IEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.utils.Pair;
import edu.uci.ics.hyracks.algebricks.core.utils.Triple;

public class AccessMethodUtils {
	public static List<Object> primaryIndexTypes(AqlCompiledDatasetDecl datasetDecl, IAType itemType) {
        List<Object> types = new ArrayList<Object>();
        List<Triple<IEvaluatorFactory, ScalarFunctionCallExpression, IAType>> partitioningFunctions = DatasetUtils
                .getPartitioningFunctions(datasetDecl);
        for (Triple<IEvaluatorFactory, ScalarFunctionCallExpression, IAType> t : partitioningFunctions) {
            types.add(t.third);
        }
        types.add(itemType);
        return types;
    }
	
	public static ConstantExpression createStringConstant(String str) {
        return new ConstantExpression(new AsterixConstantValue(new AString(str)));
    }
	
    public static boolean analyzeFuncExprArgsForOneConstAndVar(AbstractFunctionCallExpression funcExpr,
            AccessMethodAnalysisContext analysisCtx) {
        IAlgebricksConstantValue constFilterVal = null;
        LogicalVariable fieldVar = null;
        ILogicalExpression arg1 = funcExpr.getArguments().get(0).getValue();
        ILogicalExpression arg2 = funcExpr.getArguments().get(1).getValue();
        // One of the args must be a constant, and the other arg must be a
        // variable.
        if (arg1.getExpressionTag() == LogicalExpressionTag.CONSTANT
                && arg2.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
            ConstantExpression constExpr = (ConstantExpression) arg1;
            constFilterVal = constExpr.getValue();
            VariableReferenceExpression varExpr = (VariableReferenceExpression) arg2;
            fieldVar = varExpr.getVariableReference();
        } else if (arg1.getExpressionTag() == LogicalExpressionTag.VARIABLE
                && arg2.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
            ConstantExpression constExpr = (ConstantExpression) arg2;
            constFilterVal = constExpr.getValue();
            VariableReferenceExpression varExpr = (VariableReferenceExpression) arg1;
            fieldVar = varExpr.getVariableReference();
        } else {
            return false;
        }
        analysisCtx.matchedFuncExprs.add(new OptimizableBinaryFuncExpr(funcExpr, constFilterVal, fieldVar));
        return true;
    }
    
    public static boolean analyzeSimilarityCheckFuncExprArgs(AbstractFunctionCallExpression funcExpr,
            AccessMethodAnalysisContext analysisCtx) {
        // There should be exactly three arguments.
        // The last function argument is assumed to be the similarity threshold.
        IAlgebricksConstantValue constThreshVal = null;
        ILogicalExpression arg3 = funcExpr.getArguments().get(2).getValue();
        if (arg3.getExpressionTag() != LogicalExpressionTag.CONSTANT) {
            return false;
        }
        constThreshVal = ((ConstantExpression) arg3).getValue();
        ILogicalExpression arg1 = funcExpr.getArguments().get(0).getValue();
        ILogicalExpression arg2 = funcExpr.getArguments().get(1).getValue();
        // Determine whether one arg is constant, and the other is non-constant.
        ILogicalExpression constArg = null;
        ILogicalExpression nonConstArg = null;
        if (arg1.getExpressionTag() == LogicalExpressionTag.CONSTANT 
                && arg2.getExpressionTag() != LogicalExpressionTag.CONSTANT) { 
            constArg = arg1;
            nonConstArg = arg2;
        } else if(arg2.getExpressionTag() == LogicalExpressionTag.CONSTANT
                && arg1.getExpressionTag() != LogicalExpressionTag.CONSTANT) {
            constArg = arg2;
            nonConstArg = arg1;
        } else {
            return false;
        }
        IAlgebricksConstantValue constFilterVal = null;        
        LogicalVariable fieldVar = null;
        // Analyze arg1 and arg2, depending on similarity function.
        // TODO: For now, only support Jaccard.
        if (funcExpr.getFunctionIdentifier() == AsterixBuiltinFunctions.SIMILARITY_JACCARD_CHECK) {            
            ConstantExpression constExpr = (ConstantExpression) constArg;
            constFilterVal = constExpr.getValue();
            AbstractFunctionCallExpression nonConstfuncExpr = funcExpr;
            if (nonConstArg.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                nonConstfuncExpr = (AbstractFunctionCallExpression) nonConstArg;
                // TODO: Currently, we're only looking for word tokens.
                if (nonConstfuncExpr.getFunctionIdentifier() != AsterixBuiltinFunctions.WORD_TOKENS) {
                    return false;
                }
                // Find the variable that is being tokenized.
                nonConstArg = nonConstfuncExpr.getArguments().get(0).getValue();
            }
            if (nonConstArg.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
                VariableReferenceExpression varExpr = (VariableReferenceExpression) nonConstArg;
                fieldVar = varExpr.getVariableReference();
                analysisCtx.matchedFuncExprs.add(new OptimizableTernaryFuncExpr(funcExpr, constFilterVal, constThreshVal, fieldVar));
                return true;
            }
        }
        return false;
        /*
        if (funcExpr.getFunctionIdentifier() == AsterixBuiltinFunctions.EDIT_DISTANCE_CHECK) {
            return analyzeFuncExprArgsForOneConstAndVar(funcExpr, analysisCtx);
        } 
        */   
    }

    /**
     * @return A list of types corresponding to fields produced by the given
     *         index when searched.
     */
    public static List<Object> getSecondaryIndexTypes(AqlCompiledDatasetDecl datasetDecl, AqlCompiledIndexDecl index,
            ARecordType recordType, boolean primaryKeysOnly) throws AlgebricksException {
        List<Object> types = new ArrayList<Object>();
        if (!primaryKeysOnly) {
            for (String sk : index.getFieldExprs()) {
                types.add(AqlCompiledIndexDecl.keyFieldType(sk, recordType));
            }
        }
        for (Triple<IEvaluatorFactory, ScalarFunctionCallExpression, IAType> t : DatasetUtils
                .getPartitioningFunctions(datasetDecl)) {
            types.add(t.third);
        }
        return types;
    }
    
    public static UnnestMapOperator createPrimaryIndexUnnestMap(AqlCompiledDatasetDecl datasetDecl,
            ARecordType recordType, List<LogicalVariable> primaryIndexVars, AqlCompiledIndexDecl secondaryIndex,
            int numSecondaryKeys, List<Object> secondaryIndexTypes, UnnestingFunctionCallExpression rangeSearchFun,
            AssignOperator assignSearchKeys, IOptimizationContext context, boolean outputPrimaryKeysOnly,
            boolean sortPrimaryKeys) throws AlgebricksException {
        // List of variables for the primary keys coming out of a secondary-index search.
        int numPrimaryKeys = DatasetUtils.getPartitioningFunctions(datasetDecl).size();
        ArrayList<LogicalVariable> secondaryIndexPrimaryKeys = new ArrayList<LogicalVariable>(numPrimaryKeys);
        for (int i = 0; i < numPrimaryKeys; i++) {
            secondaryIndexPrimaryKeys.add(context.newVar());
        }        
        // List of variables coming out of the secondary-index search. It contains the primary keys, and optionally the secondary keys.
        ArrayList<LogicalVariable> secondaryIndexUnnestVars = new ArrayList<LogicalVariable>();
        if (!outputPrimaryKeysOnly) {
            // Add one variable per secondary-index key.
            for (int i = 0; i < numSecondaryKeys; i++) {
                secondaryIndexUnnestVars.add(context.newVar());
            }
        }
        // Add the primary keys after the secondary keys.
        secondaryIndexUnnestVars.addAll(secondaryIndexPrimaryKeys);
        // This is the operator that jobgen will be looking for. It contains an unnest function that has all necessary arguments to determine
        // which index to use, which variables contain the index-search keys, what is the original dataset, etc.
        UnnestMapOperator secondaryIndexUnnestOp = new UnnestMapOperator(secondaryIndexUnnestVars, new MutableObject<ILogicalExpression>(
                rangeSearchFun), secondaryIndexTypes);
        secondaryIndexUnnestOp.getInputs().add(new MutableObject<ILogicalOperator>(assignSearchKeys));
        secondaryIndexUnnestOp.setExecutionMode(ExecutionMode.PARTITIONED);

        // Optionally add a sort on the primary-index keys before searching the primary index.
        OrderOperator order = null;
        if (sortPrimaryKeys) {
            order = new OrderOperator();
            for (LogicalVariable v : secondaryIndexPrimaryKeys) {
                Mutable<ILogicalExpression> vRef = new MutableObject<ILogicalExpression>(new VariableReferenceExpression(v));
                order.getOrderExpressions().add(
                        new Pair<IOrder, Mutable<ILogicalExpression>>(OrderOperator.ASC_ORDER, vRef));
            }
            // The secondary-index search feeds into the sort.
            order.getInputs().add(new MutableObject<ILogicalOperator>(secondaryIndexUnnestOp));
            order.setExecutionMode(ExecutionMode.LOCAL);
        }

        // List of arguments to be passed into the primary index unnest (these arguments will be consumed by the corresponding physical rewrite rule). 
        // The arguments are: the name of the primary index, the type of index, the name of the dataset, 
        // the number of primary-index keys, and the variable references corresponding to the primary-index search keys.
        List<Mutable<ILogicalExpression>> primaryIndexFuncArgs = new ArrayList<Mutable<ILogicalExpression>>();
        primaryIndexFuncArgs.add(new MutableObject<ILogicalExpression>(AccessMethodUtils.createStringConstant(datasetDecl.getName())));
        primaryIndexFuncArgs.add(new MutableObject<ILogicalExpression>(AccessMethodUtils.createStringConstant(FunctionArgumentsConstants.BTREE_INDEX)));
        primaryIndexFuncArgs.add(new MutableObject<ILogicalExpression>(AccessMethodUtils.createStringConstant(datasetDecl.getName())));
        // Add the variables corresponding to the primary-index search keys (low key).
        primaryIndexFuncArgs.add(new MutableObject<ILogicalExpression>(new ConstantExpression(new AsterixConstantValue(new AInt32(
                numPrimaryKeys)))));
        for (LogicalVariable v : secondaryIndexPrimaryKeys) {
            primaryIndexFuncArgs.add(new MutableObject<ILogicalExpression>(new VariableReferenceExpression(v)));
        }
        // Add the variables corresponding to the primary-index search keys (high key).
        primaryIndexFuncArgs.add(new MutableObject<ILogicalExpression>(new ConstantExpression(new AsterixConstantValue(new AInt32(
                numPrimaryKeys)))));
        for (LogicalVariable v : secondaryIndexPrimaryKeys) {
            primaryIndexFuncArgs.add(new MutableObject<ILogicalExpression>(new VariableReferenceExpression(v)));
        }
        // Low key inclusive, and high key inclusive are both true, meaning the search interval is closed.
        // Since the low key and high key are also the same, we have a point lookup.
        primaryIndexFuncArgs.add(new MutableObject<ILogicalExpression>(ConstantExpression.TRUE));
        primaryIndexFuncArgs.add(new MutableObject<ILogicalExpression>(ConstantExpression.TRUE));
        IFunctionInfo primaryIndexSearch = FunctionUtils.getFunctionInfo(AsterixBuiltinFunctions.INDEX_SEARCH);
        AbstractFunctionCallExpression searchPrimIdxFun = new ScalarFunctionCallExpression(primaryIndexSearch, primaryIndexFuncArgs);
        UnnestMapOperator primaryIndexUnnestMap = new UnnestMapOperator(primaryIndexVars, new MutableObject<ILogicalExpression>(searchPrimIdxFun),
                AccessMethodUtils.primaryIndexTypes(datasetDecl, recordType));
        // Fed by the order operator or the secondaryIndexUnnestOp.
        if (sortPrimaryKeys) {
            primaryIndexUnnestMap.getInputs().add(new MutableObject<ILogicalOperator>(order));
        } else {
            primaryIndexUnnestMap.getInputs().add(new MutableObject<ILogicalOperator>(secondaryIndexUnnestOp));
        }
        primaryIndexUnnestMap.setExecutionMode(ExecutionMode.PARTITIONED);
        return primaryIndexUnnestMap;
    }
    
}

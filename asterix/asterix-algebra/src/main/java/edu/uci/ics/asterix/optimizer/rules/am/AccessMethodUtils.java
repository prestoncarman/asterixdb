package edu.uci.ics.asterix.optimizer.rules.am;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import edu.uci.ics.asterix.aql.util.FunctionUtils;
import edu.uci.ics.asterix.common.functions.FunctionArgumentsConstants;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledDatasetDecl;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledIndexDecl;
import edu.uci.ics.asterix.metadata.utils.DatasetUtils;
import edu.uci.ics.asterix.om.base.ABoolean;
import edu.uci.ics.asterix.om.base.AInt32;
import edu.uci.ics.asterix.om.base.AString;
import edu.uci.ics.asterix.om.base.IAObject;
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
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.UnnestingFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator.ExecutionMode;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.OrderOperator.IOrder;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
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
	
	public static ConstantExpression createInt32Constant(int i) {
        return new ConstantExpression(new AsterixConstantValue(new AInt32(i)));
    }
	
	public static ConstantExpression createBooleanConstant(boolean b) {
        if (b) {
            return new ConstantExpression(new AsterixConstantValue(ABoolean.TRUE));
        } else {
            return new ConstantExpression(new AsterixConstantValue(ABoolean.FALSE));
        }
    }
	
	public static String getStringConstant(Mutable<ILogicalExpression> expr) {
        IAObject obj = ((AsterixConstantValue)((ConstantExpression) expr.getValue())
                .getValue()).getObject();
        return ((AString)obj).getStringValue();
    }
	
    public static int getInt32Constant(Mutable<ILogicalExpression> expr) {
        IAObject obj = ((AsterixConstantValue)((ConstantExpression) expr.getValue())
                .getValue()).getObject();
        return ((AInt32)obj).getIntegerValue();
    }
    
    public static boolean getBooleanConstant(Mutable<ILogicalExpression> expr) {
        IAObject obj = ((AsterixConstantValue)((ConstantExpression) expr.getValue())
                .getValue()).getObject();
        return ((ABoolean)obj).getBoolean();
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
        analysisCtx.matchedFuncExprs.add(new OptimizableFuncExpr(funcExpr, fieldVar, constFilterVal));
        return true;
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
    
    public static List<LogicalVariable> getPrimaryKeyVars(List<LogicalVariable> sourceVars, int numPrimaryKeys, int numSecondaryKeys, boolean outputPrimaryKeysOnly) {
        List<LogicalVariable> primaryKeyVars = new ArrayList<LogicalVariable>();
        int start = sourceVars.size() - numPrimaryKeys;
        int stop = sourceVars.size();        
        for (int i = start; i < stop; i++) {
            primaryKeyVars.add(sourceVars.get(i));
        }
        return primaryKeyVars;
    }
    
    public static UnnestMapOperator createSecondaryIndexUnnestMap(AqlCompiledDatasetDecl datasetDecl,
            ARecordType recordType, AqlCompiledIndexDecl indexDecl, ILogicalOperator inputOp,
            ArrayList<Mutable<ILogicalExpression>> secondaryIndexFuncArgs, int numSecondaryKeys, List<Object> secondaryIndexTypes,
            IOptimizationContext context, boolean outputPrimaryKeysOnly, boolean retainInput) throws AlgebricksException {        
    	// This is the logical representation of our secondary-index search.
        // An index search is expressed logically as an unnest over an index-search function.
        IFunctionInfo secondaryIndexSearch = FunctionUtils.getFunctionInfo(AsterixBuiltinFunctions.INDEX_SEARCH);
        UnnestingFunctionCallExpression secondaryIndexSearchFunc = new UnnestingFunctionCallExpression(secondaryIndexSearch, secondaryIndexFuncArgs);
        secondaryIndexSearchFunc.setReturnsUniqueValues(true);
    	// List of variables for the primary keys coming out of a secondary-index search.
        int numPrimaryKeys = DatasetUtils.getPartitioningFunctions(datasetDecl).size();
        ArrayList<LogicalVariable> secondaryIndexPrimaryKeys = new ArrayList<LogicalVariable>(numPrimaryKeys);
        for (int i = 0; i < numPrimaryKeys; i++) {
            secondaryIndexPrimaryKeys.add(context.newVar());
        }
        // List of variables coming out of the secondary-index search. It contains the primary keys, and optionally the secondary keys.
        ArrayList<LogicalVariable> secondaryIndexUnnestVars = new ArrayList<LogicalVariable>();
        if (retainInput) {
            VariableUtilities.getLiveVariables(inputOp, secondaryIndexUnnestVars);
            List<Object> newSecondaryIndexTypes = new ArrayList<Object>();
            IVariableTypeEnvironment typeEnv = context.getOutputTypeEnvironment(inputOp);
            for (LogicalVariable var : secondaryIndexUnnestVars) {
                newSecondaryIndexTypes.add(typeEnv.getVarType(var));
            }
            newSecondaryIndexTypes.addAll(secondaryIndexTypes);
            secondaryIndexTypes = newSecondaryIndexTypes;
        }
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
                secondaryIndexSearchFunc), secondaryIndexTypes);
        secondaryIndexUnnestOp.getInputs().add(new MutableObject<ILogicalOperator>(inputOp));
        secondaryIndexUnnestOp.setExecutionMode(ExecutionMode.PARTITIONED);
        context.computeAndSetTypeEnvironmentForOperator(secondaryIndexUnnestOp);
        
        return secondaryIndexUnnestOp;
    }
    
    public static UnnestMapOperator createPrimaryIndexUnnestMap(AqlCompiledDatasetDecl datasetDecl, 
            ARecordType recordType, List<LogicalVariable> primaryIndexVars, ILogicalOperator inputOp,
            IOptimizationContext context, List<LogicalVariable> primaryKeyVars, boolean sortPrimaryKeys, boolean retainInput, boolean requiresBroadcast) throws AlgebricksException {
        // Optionally add a sort on the primary-index keys before searching the primary index.
        OrderOperator order = null;
        if (sortPrimaryKeys) {
            order = new OrderOperator();
            for (LogicalVariable pkVar : primaryKeyVars) {
                Mutable<ILogicalExpression> vRef = new MutableObject<ILogicalExpression>(new VariableReferenceExpression(pkVar));
                order.getOrderExpressions().add(
                        new Pair<IOrder, Mutable<ILogicalExpression>>(OrderOperator.ASC_ORDER, vRef));
            }
            // The secondary-index search feeds into the sort.
            order.getInputs().add(new MutableObject<ILogicalOperator>(inputOp));
            order.setExecutionMode(ExecutionMode.LOCAL);           
            context.computeAndSetTypeEnvironmentForOperator(order);
        }

        // List of arguments to be passed into the primary index unnest (these arguments will be consumed by the corresponding physical rewrite rule). 
        // The arguments are: the name of the primary index, the type of index, the name of the dataset, 
        // the number of primary-index keys, and the variable references corresponding to the primary-index search keys.
        List<Mutable<ILogicalExpression>> primaryIndexFuncArgs = new ArrayList<Mutable<ILogicalExpression>>();
        primaryIndexFuncArgs.add(new MutableObject<ILogicalExpression>(createStringConstant(datasetDecl.getName())));
        primaryIndexFuncArgs.add(new MutableObject<ILogicalExpression>(createStringConstant(FunctionArgumentsConstants.BTREE_INDEX)));
        primaryIndexFuncArgs.add(new MutableObject<ILogicalExpression>(createStringConstant(datasetDecl.getName())));
        primaryIndexFuncArgs.add(new MutableObject<ILogicalExpression>(createBooleanConstant(retainInput)));
        primaryIndexFuncArgs.add(new MutableObject<ILogicalExpression>(createBooleanConstant(requiresBroadcast)));
        // Add the variables corresponding to the primary-index search keys (low key).
        primaryIndexFuncArgs.add(new MutableObject<ILogicalExpression>(createInt32Constant(primaryKeyVars.size())));
        for (LogicalVariable pkVar : primaryKeyVars) {
            primaryIndexFuncArgs.add(new MutableObject<ILogicalExpression>(new VariableReferenceExpression(pkVar)));
        }
        // Add the variables corresponding to the primary-index search keys (high key).
        primaryIndexFuncArgs.add(new MutableObject<ILogicalExpression>(createInt32Constant(primaryKeyVars.size())));
        for (LogicalVariable pkVar : primaryKeyVars) {
            primaryIndexFuncArgs.add(new MutableObject<ILogicalExpression>(new VariableReferenceExpression(pkVar)));
        }
        // Low key inclusive, and high key inclusive are both true, meaning the search interval is closed.
        // Since the low key and high key are also the same, we have a point lookup.
        primaryIndexFuncArgs.add(new MutableObject<ILogicalExpression>(ConstantExpression.TRUE));
        primaryIndexFuncArgs.add(new MutableObject<ILogicalExpression>(ConstantExpression.TRUE));
        IFunctionInfo primaryIndexSearch = FunctionUtils.getFunctionInfo(AsterixBuiltinFunctions.INDEX_SEARCH);
        AbstractFunctionCallExpression searchPrimIdxFun = new ScalarFunctionCallExpression(primaryIndexSearch, primaryIndexFuncArgs);
        List<Object> primaryIndexTypes = AccessMethodUtils.primaryIndexTypes(datasetDecl, recordType);
        if (retainInput) {
            List<LogicalVariable> newPrimaryIndexVars = new ArrayList<LogicalVariable>();
            VariableUtilities.getLiveVariables(inputOp, newPrimaryIndexVars);            
            List<Object> newPrimaryIndexTypes = new ArrayList<Object>();
            context.computeAndSetTypeEnvironmentForOperator(inputOp);
            IVariableTypeEnvironment typeEnv = context.getOutputTypeEnvironment(inputOp);
            for (LogicalVariable var : newPrimaryIndexVars) {
                newPrimaryIndexTypes.add(typeEnv.getVarType(var));
            }
            newPrimaryIndexTypes.addAll(primaryIndexTypes);
            primaryIndexTypes = newPrimaryIndexTypes;
            newPrimaryIndexVars.addAll(primaryIndexVars);
            primaryIndexVars = newPrimaryIndexVars;
        }
        UnnestMapOperator primaryIndexUnnestOp = new UnnestMapOperator(primaryIndexVars, new MutableObject<ILogicalExpression>(searchPrimIdxFun),
                primaryIndexTypes);
        // Fed by the order operator or the secondaryIndexUnnestOp.
        if (sortPrimaryKeys) {
            primaryIndexUnnestOp.getInputs().add(new MutableObject<ILogicalOperator>(order));
        } else {
            primaryIndexUnnestOp.getInputs().add(new MutableObject<ILogicalOperator>(inputOp));
        }
        primaryIndexUnnestOp.setExecutionMode(ExecutionMode.PARTITIONED);
        return primaryIndexUnnestOp;
    }
    
}

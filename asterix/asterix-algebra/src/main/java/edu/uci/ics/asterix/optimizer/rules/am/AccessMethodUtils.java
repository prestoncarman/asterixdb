package edu.uci.ics.asterix.optimizer.rules.am;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import edu.uci.ics.asterix.aql.util.FunctionUtils;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledDatasetDecl;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledIndexDecl;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledIndexDecl.IndexKind;
import edu.uci.ics.asterix.metadata.utils.DatasetUtils;
import edu.uci.ics.asterix.om.base.ABoolean;
import edu.uci.ics.asterix.om.base.AInt32;
import edu.uci.ics.asterix.om.base.AString;
import edu.uci.ics.asterix.om.base.IAObject;
import edu.uci.ics.asterix.om.constants.AsterixConstantValue;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.util.NonTaggedFormatUtil;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.algebricks.common.utils.Triple;
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
import edu.uci.ics.hyracks.algebricks.runtime.base.IEvaluatorFactory;

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
    
    public static int getNumSecondaryKeys(AqlCompiledDatasetDecl datasetDecl, AqlCompiledIndexDecl indexDecl,
            ARecordType recordType) throws AlgebricksException {
        switch (indexDecl.getKind()) {
            case BTREE:
            case WORD_INVIX:
            case NGRAM_INVIX: {
                return indexDecl.getFieldExprs().size();
            }
            case RTREE: {
                IAType keyType = AqlCompiledIndexDecl.keyFieldType(indexDecl.getFieldExprs().get(0), recordType);
                int numDimensions = NonTaggedFormatUtil.getNumDimensions(keyType.getTypeTag());
                return numDimensions * 2;
            }
            default: {
                throw new AlgebricksException("Unknown index kind: " + indexDecl.getKind());
            }
        }
    }
    
    /**
     * @return Appends the types corresponding to fields produced by the given
     *         index when searched.
     */
    public static void getSecondaryIndexTypes(AqlCompiledDatasetDecl datasetDecl, ARecordType recordType,
            AqlCompiledIndexDecl indexDecl, boolean primaryKeysOnly, List<Object> dest) throws AlgebricksException {
        if (!primaryKeysOnly) {
            switch (indexDecl.getKind()) {
                case BTREE:
                case WORD_INVIX:
                case NGRAM_INVIX: {
                    for (String sk : indexDecl.getFieldExprs()) {
                        dest.add(AqlCompiledIndexDecl.keyFieldType(sk, recordType));
                    }
                    break;
                }
                case RTREE: {
                    IAType keyType = AqlCompiledIndexDecl.keyFieldType(indexDecl.getFieldExprs().get(0), recordType);
                    IAType nestedKeyType = NonTaggedFormatUtil.getNestedSpatialType(keyType.getTypeTag());
                    int numKeys = getNumSecondaryKeys(datasetDecl, indexDecl, recordType);
                    for (int i = 0; i < numKeys; i++) {
                        dest.add(nestedKeyType);
                    }
                    break;
                }
            }
        }
        // Primary keys.
        for (Triple<IEvaluatorFactory, ScalarFunctionCallExpression, IAType> t : DatasetUtils
                .getPartitioningFunctions(datasetDecl)) {
            dest.add(t.third);
        }
    }
    
    public static void createSecondaryIndexOutputVars(AqlCompiledDatasetDecl datasetDecl, ARecordType recordType,
            AqlCompiledIndexDecl indexDecl, boolean primaryKeysOnly, IOptimizationContext context,
            List<LogicalVariable> dest) throws AlgebricksException {
        int numPrimaryKeys = DatasetUtils.getPartitioningFunctions(datasetDecl).size();
        int numSecondaryKeys = getNumSecondaryKeys(datasetDecl, indexDecl, recordType);
        int numVars = (primaryKeysOnly) ? numPrimaryKeys : numPrimaryKeys + numSecondaryKeys;
        for (int i = 0; i < numVars; i++) {
            dest.add(context.newVar());
        }
    }
    
    public static List<LogicalVariable> getPrimaryKeyVars(List<LogicalVariable> sourceVars, int numPrimaryKeys,
            int numSecondaryKeys, boolean outputPrimaryKeysOnly) {
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
            AccessMethodJobGenParams jobGenParams, IOptimizationContext context, boolean outputPrimaryKeysOnly,
            boolean retainInput) throws AlgebricksException {
        // The job gen parameters are transferred to the actual job gen via 
        // the UnnestMapOperator's function arguments.
        ArrayList<Mutable<ILogicalExpression>> secondaryIndexFuncArgs = new ArrayList<Mutable<ILogicalExpression>>();
        jobGenParams.writeToFuncArgs(secondaryIndexFuncArgs);
        // An secondary-index search is logically expressed as an unnest over an index-search function.
        IFunctionInfo secondaryIndexSearch = FunctionUtils.getFunctionInfo(AsterixBuiltinFunctions.INDEX_SEARCH);
        UnnestingFunctionCallExpression secondaryIndexSearchFunc = new UnnestingFunctionCallExpression(secondaryIndexSearch, secondaryIndexFuncArgs);
        secondaryIndexSearchFunc.setReturnsUniqueValues(true);
        // List of variables coming out of the secondary-index search. It contains the primary keys, and optionally the secondary keys.
        List<LogicalVariable> secondaryIndexUnnestVars = new ArrayList<LogicalVariable>();
        List<Object> secondaryIndexOutputTypes = new ArrayList<Object>();
        if (retainInput) {
            VariableUtilities.getLiveVariables(inputOp, secondaryIndexUnnestVars);
            IVariableTypeEnvironment typeEnv = context.getOutputTypeEnvironment(inputOp);
            for (LogicalVariable var : secondaryIndexUnnestVars) {
                secondaryIndexOutputTypes.add(typeEnv.getVarType(var));
            }
        }
        // Fill output variables/types generated by the secondary-index search (not forwarded from input).
        AccessMethodUtils.createSecondaryIndexOutputVars(datasetDecl, recordType, indexDecl, outputPrimaryKeysOnly, context, secondaryIndexUnnestVars);
        AccessMethodUtils.getSecondaryIndexTypes(datasetDecl, recordType, indexDecl, outputPrimaryKeysOnly, secondaryIndexOutputTypes);
        // This is the operator that jobgen will be looking for. It contains an unnest function that has all necessary arguments to determine
        // which index to use, which variables contain the index-search keys, what is the original dataset, etc.
        UnnestMapOperator secondaryIndexUnnestOp = new UnnestMapOperator(secondaryIndexUnnestVars, new MutableObject<ILogicalExpression>(
                secondaryIndexSearchFunc), secondaryIndexOutputTypes);
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

        // TODO: Fix comments.
        // List of arguments to be passed into the primary index unnest (these arguments will be consumed by the corresponding physical rewrite rule). 
        // The arguments are: the name of the primary index, the type of index, the name of the dataset, 
        // the number of primary-index keys, and the variable references corresponding to the primary-index search keys.
        List<Mutable<ILogicalExpression>> primaryIndexFuncArgs = new ArrayList<Mutable<ILogicalExpression>>();
        BTreeJobGenParams jobGenParams = new BTreeJobGenParams(datasetDecl.getName(), IndexKind.BTREE, datasetDecl.getName(), retainInput, requiresBroadcast);
        // Set low/high inclusive to true for a point lookup.
        jobGenParams.setLowKeyInclusive(true);
        jobGenParams.setHighKeyInclusive(true);
        jobGenParams.setLowKeyVarList(primaryKeyVars, 0, primaryKeyVars.size());
        jobGenParams.setHighKeyVarList(primaryKeyVars, 0, primaryKeyVars.size());
        jobGenParams.writeToFuncArgs(primaryIndexFuncArgs);
        
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

package edu.uci.ics.asterix.optimizer.rules;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import edu.uci.ics.asterix.common.functions.FunctionArgumentsConstants;
import edu.uci.ics.asterix.common.functions.FunctionUtils;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledDatasetDecl;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledIndexDecl;
import edu.uci.ics.asterix.metadata.utils.DatasetUtils;
import edu.uci.ics.asterix.om.base.AInt32;
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
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions.ComparisonKind;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator.ExecutionMode;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.OrderOperator.IOrder;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.base.IEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.utils.Pair;
import edu.uci.ics.hyracks.algebricks.core.utils.Triple;

public class BTreeAccessPath implements IAccessPath {

    // Describes whether a search predicate is an open/closed interval.
    private enum LimitType {
        LOW_INCLUSIVE, LOW_EXCLUSIVE, HIGH_INCLUSIVE, HIGH_EXCLUSIVE, EQUAL
    }
    
    // TODO: There is some redundancy here, since these are listed in AlgebricksBuiltinFunctions as well.
    private static List<FunctionIdentifier> funcIdents = new ArrayList<FunctionIdentifier>();
    static {
        funcIdents.add(AlgebricksBuiltinFunctions.EQ);
        funcIdents.add(AlgebricksBuiltinFunctions.LE);
        funcIdents.add(AlgebricksBuiltinFunctions.GE);
        funcIdents.add(AlgebricksBuiltinFunctions.LT);
        funcIdents.add(AlgebricksBuiltinFunctions.GT);
        funcIdents.add(AlgebricksBuiltinFunctions.NEQ);
    }
    
    public static BTreeAccessPath INSTANCE = new BTreeAccessPath();
    
    @Override
    public List<FunctionIdentifier> getOptimizableFunctions() {
        return funcIdents;
    }
    
    @Override
    public boolean analyzeFuncExprArgs(AbstractFunctionCallExpression funcExpr, AccessPathAnalysisContext analysisCtx) {
        return AccessPathUtils.analyzeFuncExprArgsForOneConstAndVar(funcExpr, analysisCtx);
    }

    @Override
    public boolean matchAllIndexExprs() {
        return true;
    }

    @Override
    public boolean matchPrefixIndexExprs() {
        // TODO: The BTree can support this. Enable this later and add tests.
        return false;
    }

    @Override
    public boolean applyPlanTransformation(Mutable<ILogicalOperator> selectRef, Mutable<ILogicalOperator> assignRef,
            Mutable<ILogicalOperator> dataSourceScanRef, AqlCompiledDatasetDecl datasetDecl, ARecordType recordType,
            AqlCompiledIndexDecl chosenIndex, AccessPathAnalysisContext analysisCtx, IOptimizationContext context) 
                    throws AlgebricksException {
        SelectOperator select = (SelectOperator) selectRef.getValue();
        DataSourceScanOperator dataSourceScan = (DataSourceScanOperator) dataSourceScanRef.getValue();
        AssignOperator assign = null;
        if (assignRef != null) {
            assign = (AssignOperator) assignRef.getValue();
        }
        int numKeys = chosenIndex.getFieldExprs().size();
        IAlgebricksConstantValue[] lowKeyConstants = new IAlgebricksConstantValue[numKeys];
        IAlgebricksConstantValue[] highKeyConstants = new IAlgebricksConstantValue[numKeys];
        LimitType[] lowKeyLimits = new LimitType[numKeys];
        LimitType[] highKeyLimits = new LimitType[numKeys];
        boolean[] lowKeyInclusive = new boolean[numKeys];
        boolean[] highKeyInclusive = new boolean[numKeys];
        List<Integer> exprList = analysisCtx.indexExprs.get(chosenIndex);
        List<OptimizableFuncExpr> matchedFuncExprs = analysisCtx.matchedFuncExprs;
                
        Set<ILogicalExpression> replacedFuncExprs = new HashSet<ILogicalExpression>();
        boolean couldntFigureOut = false;
        boolean doneWithExprs = false;
        for (Integer exprIndex : exprList) {
            int keyPos = indexOf(matchedFuncExprs.get(exprIndex).getFieldName(), chosenIndex.getFieldExprs());
            if (keyPos < 0) {
                throw new InternalError();
            }
            LimitType limit = getLimitType(matchedFuncExprs.get(exprIndex));
            switch (limit) {
                case EQUAL: {
                    if (lowKeyLimits[keyPos] == null && highKeyLimits[keyPos] == null) {
                        lowKeyLimits[keyPos] = highKeyLimits[keyPos] = limit;
                        lowKeyInclusive[keyPos] = highKeyInclusive[keyPos] = true;
                        lowKeyConstants[keyPos] = highKeyConstants[keyPos] = matchedFuncExprs.get(exprIndex).getConstVal();
                    } else {
                        couldntFigureOut = true;
                    }
                    // Mmmm, we would need an inference system here.
                    doneWithExprs = true;
                    break;
                }
                case HIGH_EXCLUSIVE: {
                    if (highKeyLimits[keyPos] == null || (highKeyLimits[keyPos] != null && highKeyInclusive[keyPos])) {
                        highKeyLimits[keyPos] = limit;
                        highKeyConstants[keyPos] = matchedFuncExprs.get(exprIndex).getConstVal();
                        highKeyInclusive[keyPos] = false;
                    } else {
                        couldntFigureOut = true;
                        doneWithExprs = true;
                    }
                    break;
                }
                case HIGH_INCLUSIVE: {
                    if (highKeyLimits[keyPos] == null) {
                        highKeyLimits[keyPos] = limit;
                        highKeyConstants[keyPos] = matchedFuncExprs.get(exprIndex).getConstVal();
                        highKeyInclusive[keyPos] = true;
                    } else {
                        couldntFigureOut = true;
                        doneWithExprs = true;
                    }
                    break;
                }
                case LOW_EXCLUSIVE: {
                    if (lowKeyLimits[keyPos] == null || (lowKeyLimits[keyPos] != null && lowKeyInclusive[keyPos])) {
                        lowKeyLimits[keyPos] = limit;
                        lowKeyConstants[keyPos] = matchedFuncExprs.get(exprIndex).getConstVal();
                        lowKeyInclusive[keyPos] = false;
                    } else {
                        couldntFigureOut = true;
                        doneWithExprs = true;
                    }
                    break;
                }
                case LOW_INCLUSIVE: {
                    if (lowKeyLimits[keyPos] == null) {
                        lowKeyLimits[keyPos] = limit;
                        lowKeyConstants[keyPos] = matchedFuncExprs.get(exprIndex).getConstVal();
                        lowKeyInclusive[keyPos] = true;
                    } else {
                        couldntFigureOut = true;
                        doneWithExprs = true;
                    }
                    break;
                }
                default: {
                    throw new IllegalStateException();
                }
            }
            if (!couldntFigureOut) {
                replacedFuncExprs.add(matchedFuncExprs.get(exprIndex).getFuncExpr());
            }
            if (doneWithExprs) {
                break;
            }
        }
        if (couldntFigureOut) {
            return false;
        }

        // Rule out the cases unsupported by the current btree search
        // implementation.
        for (int i = 1; i < numKeys; i++) {
            if (lowKeyInclusive[i] != lowKeyInclusive[0] || highKeyInclusive[i] != highKeyInclusive[0]) {
                return false;
            }
            if (lowKeyLimits[0] == null && lowKeyLimits[i] != null || lowKeyLimits[0] != null && lowKeyLimits[i] == null) {
                return false;
            }
            if (highKeyLimits[0] == null && highKeyLimits[i] != null || highKeyLimits[0] != null && highKeyLimits[i] == null) {
                return false;
            }
        }
        if (lowKeyLimits[0] == null) {
            lowKeyInclusive[0] = true;
        }
        if (highKeyLimits[0] == null) {
            highKeyInclusive[0] = true;
        }

        ArrayList<Mutable<ILogicalExpression>> secondaryIndexFuncArgs = new ArrayList<Mutable<ILogicalExpression>>();
        secondaryIndexFuncArgs.add(new MutableObject<ILogicalExpression>(AccessPathUtils.createStringConstant(chosenIndex.getIndexName())));
        secondaryIndexFuncArgs.add(new MutableObject<ILogicalExpression>(AccessPathUtils.createStringConstant(FunctionArgumentsConstants.BTREE_INDEX)));
        secondaryIndexFuncArgs.add(new MutableObject<ILogicalExpression>(AccessPathUtils.createStringConstant(datasetDecl.getName())));

        ArrayList<Mutable<ILogicalExpression>> keyExprList = new ArrayList<Mutable<ILogicalExpression>>();
        ArrayList<LogicalVariable> keyVarList = new ArrayList<LogicalVariable>();
        createKeyVarsAndExprs(lowKeyLimits, lowKeyConstants, keyExprList, keyVarList, secondaryIndexFuncArgs, context);
        createKeyVarsAndExprs(highKeyLimits, highKeyConstants, keyExprList, keyVarList, secondaryIndexFuncArgs, context);

        // Set low and high key inclusive for secondary-index search.
        ILogicalExpression lowKeyExpr = lowKeyInclusive[0] ? ConstantExpression.TRUE : ConstantExpression.FALSE;
        secondaryIndexFuncArgs.add(new MutableObject<ILogicalExpression>(lowKeyExpr));
        ILogicalExpression highKeyExpr = highKeyInclusive[0] ? ConstantExpression.TRUE : ConstantExpression.FALSE;
        secondaryIndexFuncArgs.add(new MutableObject<ILogicalExpression>(highKeyExpr));

        AssignOperator assignSearchKeys = new AssignOperator(keyVarList, keyExprList);
        assignSearchKeys.getInputs().add(dataSourceScan.getInputs().get(0));
        assignSearchKeys.setExecutionMode(dataSourceScan.getExecutionMode());

        IFunctionInfo secondaryIndexSearch = FunctionUtils.getFunctionInfo(AsterixBuiltinFunctions.INDEX_SEARCH);
        UnnestingFunctionCallExpression rangeSearchFun = new UnnestingFunctionCallExpression(secondaryIndexSearch, secondaryIndexFuncArgs);
        rangeSearchFun.setReturnsUniqueValues(true);

        List<LogicalVariable> primaryIndexVars = dataSourceScan.getVariables();
        UnnestMapOperator primaryIndexUnnestMap;
        boolean isPrimaryIndex = chosenIndex == DatasetUtils.getPrimaryIndex(datasetDecl);
        if (!isPrimaryIndex) {
            primaryIndexUnnestMap = createPrimaryIndexUnnestMap(datasetDecl, recordType, primaryIndexVars, chosenIndex,
                    rangeSearchFun, assignSearchKeys, context);
        } else {
            primaryIndexUnnestMap = new UnnestMapOperator(primaryIndexVars, new MutableObject<ILogicalExpression>(rangeSearchFun),
                    AccessPathUtils.primaryIndexTypes(datasetDecl, recordType));
            primaryIndexUnnestMap.getInputs().add(new MutableObject<ILogicalOperator>(assignSearchKeys));
        }
        primaryIndexUnnestMap.setExecutionMode(ExecutionMode.PARTITIONED);

        List<Mutable<ILogicalExpression>> remainingFuncExprs = new ArrayList<Mutable<ILogicalExpression>>();
        getNewSelectCondition(select, replacedFuncExprs, remainingFuncExprs);
        if (!remainingFuncExprs.isEmpty()) {
            ILogicalExpression pulledCond = createSelectCondition(remainingFuncExprs);
            SelectOperator selectRest = new SelectOperator(new MutableObject<ILogicalExpression>(pulledCond));            
            if (assign != null) {
                dataSourceScanRef.setValue(primaryIndexUnnestMap);
                selectRest.getInputs().add(new MutableObject<ILogicalOperator>(assign));
            } else {
                selectRest.getInputs().add(new MutableObject<ILogicalOperator>(primaryIndexUnnestMap));
            }
            selectRest.setExecutionMode(((AbstractLogicalOperator) selectRef.getValue()).getExecutionMode());
            selectRef.setValue(selectRest);
        } else {
            primaryIndexUnnestMap.setExecutionMode(ExecutionMode.PARTITIONED);
            if (assign != null) {
                dataSourceScanRef.setValue(primaryIndexUnnestMap);
                selectRef.setValue(assign);
            } else {
                selectRef.setValue(primaryIndexUnnestMap);
            }
        }
        return true;
    }

    private UnnestMapOperator createPrimaryIndexUnnestMap(AqlCompiledDatasetDecl datasetDecl, ARecordType recordType,
            List<LogicalVariable> primaryIndexVars, AqlCompiledIndexDecl secondaryIndex, UnnestingFunctionCallExpression rangeSearchFun,
            AssignOperator assignSearchKeys, IOptimizationContext context) throws AlgebricksException {
        int numPrimaryKeys = DatasetUtils.getPartitioningFunctions(datasetDecl).size();
        int numSecondaryKeys = secondaryIndex.getFieldExprs().size();
        // List of variables for the primary keys coming out of a secondary-index search.
        ArrayList<LogicalVariable> secondaryIndexPrimaryKeys = new ArrayList<LogicalVariable>(numPrimaryKeys);
        for (int i = 0; i < numPrimaryKeys; i++) {
            secondaryIndexPrimaryKeys.add(context.newVar());
        }
        // List of variables coming out of the secondary-index search. It contains the secondary keys, and the primary keys.
        ArrayList<LogicalVariable> secondaryIndexUnnestVars = new ArrayList<LogicalVariable>(numSecondaryKeys
                + secondaryIndexPrimaryKeys.size());
        // Add one variable per secondary-index key.
        for (int i = 0; i < numSecondaryKeys; i++) {
            secondaryIndexUnnestVars.add(context.newVar());
        }
        // Add the primary keys after the secondary keys.
        secondaryIndexUnnestVars.addAll(secondaryIndexPrimaryKeys);
        // This is the operator that the physical rewrite will be looking for. It contains an unnest function that has all necessary arguments to determine
        // which index to use, which variables contain the index-search keys, what is the original dataset, etc.
        UnnestMapOperator secondaryIndexUnnestOp = new UnnestMapOperator(secondaryIndexUnnestVars, new MutableObject<ILogicalExpression>(
                rangeSearchFun), secondaryIndexTypes(datasetDecl, secondaryIndex, recordType));
        secondaryIndexUnnestOp.getInputs().add(new MutableObject<ILogicalOperator>(assignSearchKeys));
        secondaryIndexUnnestOp.setExecutionMode(ExecutionMode.PARTITIONED);

        // Add a sort on the primary-index keys before searching the primary index.
        OrderOperator order = new OrderOperator();
        for (LogicalVariable v : secondaryIndexPrimaryKeys) {
            Mutable<ILogicalExpression> vRef = new MutableObject<ILogicalExpression>(new VariableReferenceExpression(v));
            order.getOrderExpressions().add(
                    new Pair<IOrder, Mutable<ILogicalExpression>>(OrderOperator.ASC_ORDER, vRef));
        }
        // The secondary-index search feeds into the sort.
        order.getInputs().add(new MutableObject<ILogicalOperator>(secondaryIndexUnnestOp));
        order.setExecutionMode(ExecutionMode.LOCAL);

        // List of arguments to be passed into the primary index unnest (these arguments will be consumed by the corresponding physical rewrite rule). 
        // The arguments are: the name of the primary index, the type of index, the name of the dataset, 
        // the number of primary-index keys, and the variable references corresponding to the primary-index search keys.
        List<Mutable<ILogicalExpression>> primaryIndexFuncArgs = new ArrayList<Mutable<ILogicalExpression>>();
        primaryIndexFuncArgs.add(new MutableObject<ILogicalExpression>(AccessPathUtils.createStringConstant(datasetDecl.getName())));
        primaryIndexFuncArgs.add(new MutableObject<ILogicalExpression>(AccessPathUtils.createStringConstant(FunctionArgumentsConstants.BTREE_INDEX)));
        primaryIndexFuncArgs.add(new MutableObject<ILogicalExpression>(AccessPathUtils.createStringConstant(datasetDecl.getName())));
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
                AccessPathUtils.primaryIndexTypes(datasetDecl, recordType));
        // Fed by the order operator.
        primaryIndexUnnestMap.getInputs().add(new MutableObject<ILogicalOperator>(order));
        return primaryIndexUnnestMap;
    }
    
    private void createKeyVarsAndExprs(LimitType[] keyLimits, IAlgebricksConstantValue[] keyConstants,
            ArrayList<Mutable<ILogicalExpression>> keyExprList, ArrayList<LogicalVariable> keyVarList,
            ArrayList<Mutable<ILogicalExpression>> secondaryIndexFuncArgs, IOptimizationContext context) {
        int numKeys = keyLimits.length;
        if (keyLimits[0] != null) {
            Mutable<ILogicalExpression> numKeysRef = new MutableObject<ILogicalExpression>(new ConstantExpression(
                    new AsterixConstantValue(new AInt32(numKeys))));
            secondaryIndexFuncArgs.add(numKeysRef);
            for (int i = 0; i < numKeys; i++) {
                LogicalVariable lowKeyVar = context.newVar();
                keyVarList.add(lowKeyVar);
                keyExprList.add(new MutableObject<ILogicalExpression>(new ConstantExpression(keyConstants[i])));
                Mutable<ILogicalExpression> lowKeyVarRef = new MutableObject<ILogicalExpression>(
                        new VariableReferenceExpression(lowKeyVar));
                secondaryIndexFuncArgs.add(lowKeyVarRef);
            }
        } else {
            Mutable<ILogicalExpression> zeroRef = new MutableObject<ILogicalExpression>(new ConstantExpression(
                    new AsterixConstantValue(new AInt32(0))));
            secondaryIndexFuncArgs.add(zeroRef);
        }
    }
    
    private void getNewSelectCondition(SelectOperator select, Set<ILogicalExpression> replacedFuncExprs, List<Mutable<ILogicalExpression>> remainingFuncExprs) {
        remainingFuncExprs.clear();
        if (replacedFuncExprs.isEmpty()) {
            return;
        }
        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) select.getCondition().getValue();
        if (replacedFuncExprs.size() == 1) {
            Iterator<ILogicalExpression> it = replacedFuncExprs.iterator();
            if (!it.hasNext()) {
                return;
            }
            if (funcExpr == it.next()) {
                // There are no remaining function exprs.
                return;
            }
        }
        // The original select cond must be an AND. Check it just to be sure.
        if (funcExpr.getFunctionIdentifier() != AlgebricksBuiltinFunctions.AND) {
            throw new IllegalStateException();
        }
        // Clean the conjuncts.
        for (Mutable<ILogicalExpression> arg : funcExpr.getArguments()) {
            ILogicalExpression argExpr = arg.getValue();
            if (argExpr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
                continue;
            }
            // If the function expression was not replaced by the new index
            // plan, then add it to the list of remaining function expressions.
            if (!replacedFuncExprs.contains(argExpr)) {
                remainingFuncExprs.add(arg);
            }
        }
    }
    
    private <T> int indexOf(T value, List<T> coll) {
        int i = 0;
        for (T member : coll) {
            if (member.equals(value)) {
                return i;
            }
            i++;
        }
        return -1;
    }
    
    private LimitType getLimitType(OptimizableFuncExpr optFuncExpr) {
        ComparisonKind ck = AlgebricksBuiltinFunctions.getComparisonType(optFuncExpr.getFuncExpr().getFunctionIdentifier());
        LimitType limit = null;
        switch (ck) {
            case EQ: {
                limit = LimitType.EQUAL;
                break;
            }
            case GE: {
                limit = optFuncExpr.constantIsOnLhs() ? LimitType.HIGH_INCLUSIVE : LimitType.LOW_INCLUSIVE;
                break;
            }
            case GT: {
                limit = optFuncExpr.constantIsOnLhs() ? LimitType.HIGH_EXCLUSIVE : LimitType.LOW_EXCLUSIVE;
                break;
            }
            case LE: {
                limit = optFuncExpr.constantIsOnLhs() ? LimitType.LOW_INCLUSIVE : LimitType.HIGH_INCLUSIVE;
                break;
            }
            case LT: {
                limit = optFuncExpr.constantIsOnLhs() ? LimitType.LOW_EXCLUSIVE : LimitType.HIGH_EXCLUSIVE;
                break;
            }
            case NEQ: {
                limit = null;
                break;
            }
            default: {
                throw new IllegalStateException();
            }
        }
        return limit;
    }
    
    /**
     * @return A list of types corresponding to fields produced by the given
     *         index when searched.
     */
    private static List<Object> secondaryIndexTypes(AqlCompiledDatasetDecl datasetDecl, AqlCompiledIndexDecl index,
            ARecordType recordType) throws AlgebricksException {
        List<Object> types = new ArrayList<Object>();
        for (String sk : index.getFieldExprs()) {
            types.add(AqlCompiledIndexDecl.keyFieldType(sk, recordType));
        }
        for (Triple<IEvaluatorFactory, ScalarFunctionCallExpression, IAType> t : DatasetUtils
                .getPartitioningFunctions(datasetDecl)) {
            types.add(t.third);
        }
        return types;
    }
    
    private ILogicalExpression createSelectCondition(List<Mutable<ILogicalExpression>> predList) {
        if (predList.size() > 1) {
            IFunctionInfo finfo = AlgebricksBuiltinFunctions.getBuiltinFunctionInfo(AlgebricksBuiltinFunctions.AND);
            return new ScalarFunctionCallExpression(finfo, predList);
        }
        return predList.get(0).getValue();
    }
}

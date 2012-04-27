package edu.uci.ics.asterix.optimizer.rules.am;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import edu.uci.ics.asterix.common.functions.FunctionArgumentsConstants;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledDatasetDecl;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledIndexDecl;
import edu.uci.ics.asterix.metadata.utils.DatasetUtils;
import edu.uci.ics.asterix.om.base.AInt32;
import edu.uci.ics.asterix.om.constants.AsterixConstantValue;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IAlgebricksConstantValue;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions.ComparisonKind;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator.ExecutionMode;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;

public class BTreeAccessMethod implements IAccessMethod {

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
    
    public static BTreeAccessMethod INSTANCE = new BTreeAccessMethod();
    
    @Override
    public List<FunctionIdentifier> getOptimizableFunctions() {
        return funcIdents;
    }
    
    @Override
    public boolean analyzeFuncExprArgs(AbstractFunctionCallExpression funcExpr, List<AssignOperator> assigns, AccessMethodAnalysisContext analysisCtx) {
        return AccessMethodUtils.analyzeFuncExprArgsForOneConstAndVar(funcExpr, analysisCtx);
    }

    @Override
    public boolean matchAllIndexExprs() {
        return true;
    }

    @Override
    public boolean matchPrefixIndexExprs() {
        // TODO: The BTree can support prefix searches. Enable this later and add tests.
        return false;
    }

    @Override
    public boolean applySelectPlanTransformation(Mutable<ILogicalOperator> selectRef, OptimizableOperatorSubTree subTree,
            AqlCompiledIndexDecl chosenIndex, AccessMethodAnalysisContext analysisCtx, IOptimizationContext context) 
                    throws AlgebricksException {
        AqlCompiledDatasetDecl datasetDecl = subTree.datasetDecl;
        ARecordType recordType = subTree.recordType;
        SelectOperator select = (SelectOperator) selectRef.getValue();
        DataSourceScanOperator dataSourceScan = subTree.dataSourceScan;
        Mutable<ILogicalOperator> assignRef = (subTree.assignRefs.isEmpty()) ? null : subTree.assignRefs.get(0);
        AssignOperator assign = null;
        if (assignRef != null) {
            assign = (AssignOperator) assignRef.getValue();
        }        
        int numSecondaryKeys = chosenIndex.getFieldExprs().size();        
        
        // Info on high and low keys for the BTree search predicate.
        IAlgebricksConstantValue[] lowKeyConstants = new IAlgebricksConstantValue[numSecondaryKeys];
        IAlgebricksConstantValue[] highKeyConstants = new IAlgebricksConstantValue[numSecondaryKeys];
        LimitType[] lowKeyLimits = new LimitType[numSecondaryKeys];
        LimitType[] highKeyLimits = new LimitType[numSecondaryKeys];
        boolean[] lowKeyInclusive = new boolean[numSecondaryKeys];
        boolean[] highKeyInclusive = new boolean[numSecondaryKeys];
        
        List<Integer> exprList = analysisCtx.indexExprs.get(chosenIndex);
        List<IOptimizableFuncExpr> matchedFuncExprs = analysisCtx.matchedFuncExprs;
        // List of function expressions that will be replaced by the secondary-index search.
        // These func exprs will be removed from the select condition at the very end of this method.
        Set<ILogicalExpression> replacedFuncExprs = new HashSet<ILogicalExpression>();
        // TODO: For now we don't do any sophisticated analysis of the func exprs to come up with "the best" range predicate.
        // If we can't figure out how to integrate a certain funcExpr into the current predicate, we just bail by setting this flag.
        boolean couldntFigureOut = false;
        boolean doneWithExprs = false;
        // Go through the func exprs listed as optimizable by the chosen index, 
        // and formulate a range predicate on the secondary-index keys.
        for (Integer exprIndex : exprList) {
            // Position of the field of matchedFuncExprs.get(exprIndex) in the chosen index's indexed exprs.
            IOptimizableFuncExpr optFuncExpr = matchedFuncExprs.get(exprIndex);
            int keyPos = indexOf(optFuncExpr.getFieldName(0), chosenIndex.getFieldExprs());
            if (keyPos < 0) {
                throw new InternalError();
            }
            LimitType limit = getLimitType(optFuncExpr);
            switch (limit) {
                case EQUAL: {
                    if (lowKeyLimits[keyPos] == null && highKeyLimits[keyPos] == null) {
                        lowKeyLimits[keyPos] = highKeyLimits[keyPos] = limit;
                        lowKeyInclusive[keyPos] = highKeyInclusive[keyPos] = true;
                        lowKeyConstants[keyPos] = highKeyConstants[keyPos] = optFuncExpr.getConstantVal(0);
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
                        highKeyConstants[keyPos] = optFuncExpr.getConstantVal(0);
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
                        highKeyConstants[keyPos] = optFuncExpr.getConstantVal(0);
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
                        lowKeyConstants[keyPos] = optFuncExpr.getConstantVal(0);
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
                        lowKeyConstants[keyPos] = optFuncExpr.getConstantVal(0);
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
                // Remember to remove this funcExpr later.
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
        for (int i = 1; i < numSecondaryKeys; i++) {
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
        
        // List of arguments to be passed into an unnest.
        // This logical rewrite rule, and the corresponding runtime op generated in the jobgen 
        // have a contract as to what goes into these arguments.
        // Here, we put the name of the chosen index, the type of index, the name of the dataset, 
        // the number of secondary-index keys, and the variable references corresponding to the secondary-index search keys.
        ArrayList<Mutable<ILogicalExpression>> secondaryIndexFuncArgs = new ArrayList<Mutable<ILogicalExpression>>();
        secondaryIndexFuncArgs.add(new MutableObject<ILogicalExpression>(AccessMethodUtils.createStringConstant(chosenIndex.getIndexName())));
        secondaryIndexFuncArgs.add(new MutableObject<ILogicalExpression>(AccessMethodUtils.createStringConstant(FunctionArgumentsConstants.BTREE_INDEX)));
        secondaryIndexFuncArgs.add(new MutableObject<ILogicalExpression>(AccessMethodUtils.createStringConstant(datasetDecl.getName())));
        // TODO: For now retainInput is always false.
        secondaryIndexFuncArgs.add(new MutableObject<ILogicalExpression>(AccessMethodUtils.createBooleanConstant(false)));
        // TODO: For now requiresBroadcast is always false.
        secondaryIndexFuncArgs.add(new MutableObject<ILogicalExpression>(AccessMethodUtils.createBooleanConstant(false)));
        // Here we generate vars and funcs for assigning the secondary-index keys to be fed into the secondary-index search.
        // List of variables for the assign.
        ArrayList<LogicalVariable> keyVarList = new ArrayList<LogicalVariable>();
        // List of expressions for the assign.
        ArrayList<Mutable<ILogicalExpression>> keyExprList = new ArrayList<Mutable<ILogicalExpression>>();
        createKeyVarsAndExprs(lowKeyLimits, lowKeyConstants, keyExprList, keyVarList, secondaryIndexFuncArgs, context);
        createKeyVarsAndExprs(highKeyLimits, highKeyConstants, keyExprList, keyVarList, secondaryIndexFuncArgs, context);

        // Set low and high key inclusive for secondary-index search.
        ILogicalExpression lowKeyExpr = lowKeyInclusive[0] ? ConstantExpression.TRUE : ConstantExpression.FALSE;
        secondaryIndexFuncArgs.add(new MutableObject<ILogicalExpression>(lowKeyExpr));
        ILogicalExpression highKeyExpr = highKeyInclusive[0] ? ConstantExpression.TRUE : ConstantExpression.FALSE;
        secondaryIndexFuncArgs.add(new MutableObject<ILogicalExpression>(highKeyExpr));

        // Assign operator that sets the secondary-index search-key fields.
        AssignOperator assignSearchKeys = new AssignOperator(keyVarList, keyExprList);
        // Input to this assign is the EmptyTupleSource (which the dataSourceScan also must have had as input).
        assignSearchKeys.getInputs().add(dataSourceScan.getInputs().get(0));
        assignSearchKeys.setExecutionMode(dataSourceScan.getExecutionMode());
        
        List<Object> secondaryIndexTypes = AccessMethodUtils.getSecondaryIndexTypes(datasetDecl, chosenIndex, recordType, false);
        UnnestMapOperator secondaryIndexUnnestOp = AccessMethodUtils.createSecondaryIndexUnnestMap(datasetDecl,
                recordType, chosenIndex, assignSearchKeys, secondaryIndexFuncArgs, numSecondaryKeys, secondaryIndexTypes, context, false,
                false);
        
        // Generate the rest of the upstream plan which feeds the search results into the primary index.
        List<LogicalVariable> primaryIndexVars = dataSourceScan.getVariables();
        UnnestMapOperator primaryIndexUnnestOp;
        boolean isPrimaryIndex = chosenIndex == DatasetUtils.getPrimaryIndex(datasetDecl);
        if (!isPrimaryIndex) {
            int numPrimaryKeys = DatasetUtils.getPartitioningFunctions(datasetDecl).size();
            List<LogicalVariable> primaryKeyVars = AccessMethodUtils.getPrimaryKeyVars(secondaryIndexUnnestOp.getVariables(), numPrimaryKeys, numSecondaryKeys, false);
            primaryIndexUnnestOp = AccessMethodUtils.createPrimaryIndexUnnestMap(datasetDecl, recordType, primaryIndexVars, secondaryIndexUnnestOp, context, primaryKeyVars, true, false, false);
        } else {
            primaryIndexUnnestOp = new UnnestMapOperator(primaryIndexVars, secondaryIndexUnnestOp.getExpressionRef(),
                    AccessMethodUtils.primaryIndexTypes(datasetDecl, recordType));
            primaryIndexUnnestOp.getInputs().add(new MutableObject<ILogicalOperator>(assignSearchKeys));
        }        

        List<Mutable<ILogicalExpression>> remainingFuncExprs = new ArrayList<Mutable<ILogicalExpression>>();
        getNewSelectExprs(select, replacedFuncExprs, remainingFuncExprs);
        // Generate new select using the new condition.
        if (!remainingFuncExprs.isEmpty()) {
            ILogicalExpression pulledCond = createSelectCondition(remainingFuncExprs);
            SelectOperator selectRest = new SelectOperator(new MutableObject<ILogicalExpression>(pulledCond));
            if (assign != null) {
                subTree.dataSourceScanRef.setValue(primaryIndexUnnestOp);
                selectRest.getInputs().add(new MutableObject<ILogicalOperator>(assign));
            } else {
                selectRest.getInputs().add(new MutableObject<ILogicalOperator>(primaryIndexUnnestOp));
            }
            selectRest.setExecutionMode(((AbstractLogicalOperator) selectRef.getValue()).getExecutionMode());
            selectRef.setValue(selectRest);
        } else {
            primaryIndexUnnestOp.setExecutionMode(ExecutionMode.PARTITIONED);
            if (assign != null) {
                subTree.dataSourceScanRef.setValue(primaryIndexUnnestOp);
                selectRef.setValue(assign);
            } else {
                selectRef.setValue(primaryIndexUnnestOp);
            }
        }
        return true;
    }
    
    @Override
    public boolean applyJoinPlanTransformation(Mutable<ILogicalOperator> joinRef,
            OptimizableOperatorSubTree leftSubTree, OptimizableOperatorSubTree rightSubTree,
            AqlCompiledIndexDecl chosenIndex, AccessMethodAnalysisContext analysisCtx, IOptimizationContext context)
            throws AlgebricksException {
        // TODO Implement this.
        return false;
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
                LogicalVariable keyVar = context.newVar();
                keyVarList.add(keyVar);
                keyExprList.add(new MutableObject<ILogicalExpression>(new ConstantExpression(keyConstants[i])));
                Mutable<ILogicalExpression> keyVarRef = new MutableObject<ILogicalExpression>(
                        new VariableReferenceExpression(keyVar));
                secondaryIndexFuncArgs.add(keyVarRef);
            }
        } else {
            Mutable<ILogicalExpression> zeroRef = new MutableObject<ILogicalExpression>(new ConstantExpression(
                    new AsterixConstantValue(new AInt32(0))));
            secondaryIndexFuncArgs.add(zeroRef);
        }
    }
    
    private void getNewSelectExprs(SelectOperator select, Set<ILogicalExpression> replacedFuncExprs, List<Mutable<ILogicalExpression>> remainingFuncExprs) {
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
    
    private LimitType getLimitType(IOptimizableFuncExpr optFuncExpr) {
        ComparisonKind ck = AlgebricksBuiltinFunctions.getComparisonType(optFuncExpr.getFuncExpr().getFunctionIdentifier());
        LimitType limit = null;
        switch (ck) {
            case EQ: {
                limit = LimitType.EQUAL;
                break;
            }
            case GE: {
                limit = constantIsOnLhs(optFuncExpr) ? LimitType.HIGH_INCLUSIVE : LimitType.LOW_INCLUSIVE;
                break;
            }
            case GT: {
                limit = constantIsOnLhs(optFuncExpr) ? LimitType.HIGH_EXCLUSIVE : LimitType.LOW_EXCLUSIVE;
                break;
            }
            case LE: {
                limit = constantIsOnLhs(optFuncExpr) ? LimitType.LOW_INCLUSIVE : LimitType.HIGH_INCLUSIVE;
                break;
            }
            case LT: {
                limit = constantIsOnLhs(optFuncExpr) ? LimitType.LOW_EXCLUSIVE : LimitType.HIGH_EXCLUSIVE;
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
    
    // Returns true if the constant value is on the "left hand side" (assuming a binary function).
    public boolean constantIsOnLhs(IOptimizableFuncExpr optFuncExpr) {
        return optFuncExpr.getFuncExpr().getArguments().get(0) == optFuncExpr.getConstantVal(0);
    }

    
    private ILogicalExpression createSelectCondition(List<Mutable<ILogicalExpression>> predList) {
        if (predList.size() > 1) {
            IFunctionInfo finfo = AsterixBuiltinFunctions.getAsterixFunctionInfo(AlgebricksBuiltinFunctions.AND);
            return new ScalarFunctionCallExpression(finfo, predList);
        }
        return predList.get(0).getValue();
    }

    @Override
    public boolean exprIsOptimizable(AqlCompiledIndexDecl index, IOptimizableFuncExpr optFuncExpr) {
        // No additional analysis required.
        return true;
    }
}

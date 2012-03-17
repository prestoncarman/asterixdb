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
import edu.uci.ics.asterix.metadata.declared.AqlCompiledMetadataDeclarations;
import edu.uci.ics.asterix.metadata.declared.AqlMetadataProvider;
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
        IAlgebricksConstantValue[] loFilter = new IAlgebricksConstantValue[numKeys];
        IAlgebricksConstantValue[] hiFilter = new IAlgebricksConstantValue[numKeys];
        LimitType[] loLimit = new LimitType[numKeys];
        LimitType[] hiLimit = new LimitType[numKeys];
        boolean[] loInclusive = new boolean[numKeys];
        boolean[] hiInclusive = new boolean[numKeys];
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
                    if (loLimit[keyPos] == null && hiLimit[keyPos] == null) {
                        loLimit[keyPos] = hiLimit[keyPos] = limit;
                        loInclusive[keyPos] = hiInclusive[keyPos] = true;
                        loFilter[keyPos] = hiFilter[keyPos] = matchedFuncExprs.get(exprIndex).getConstVal();
                    } else {
                        couldntFigureOut = true;
                    }
                    // Mmmm, we would need an inference system here.
                    doneWithExprs = true;
                    break;
                }
                case HIGH_EXCLUSIVE: {
                    if (hiLimit[keyPos] == null || (hiLimit[keyPos] != null && hiInclusive[keyPos])) {
                        hiLimit[keyPos] = limit;
                        hiFilter[keyPos] = matchedFuncExprs.get(exprIndex).getConstVal();
                        hiInclusive[keyPos] = false;
                    } else {
                        couldntFigureOut = true;
                        doneWithExprs = true;
                    }
                    break;
                }
                case HIGH_INCLUSIVE: {
                    if (hiLimit[keyPos] == null) {
                        hiLimit[keyPos] = limit;
                        hiFilter[keyPos] = matchedFuncExprs.get(exprIndex).getConstVal();
                        hiInclusive[keyPos] = true;
                    } else {
                        couldntFigureOut = true;
                        doneWithExprs = true;
                    }
                    break;
                }
                case LOW_EXCLUSIVE: {
                    if (loLimit[keyPos] == null || (loLimit[keyPos] != null && loInclusive[keyPos])) {
                        loLimit[keyPos] = limit;
                        loFilter[keyPos] = matchedFuncExprs.get(exprIndex).getConstVal();
                        loInclusive[keyPos] = false;
                    } else {
                        couldntFigureOut = true;
                        doneWithExprs = true;
                    }
                    break;
                }
                case LOW_INCLUSIVE: {
                    if (loLimit[keyPos] == null) {
                        loLimit[keyPos] = limit;
                        loFilter[keyPos] = matchedFuncExprs.get(exprIndex).getConstVal();
                        loInclusive[keyPos] = true;
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
            if (loInclusive[i] != loInclusive[0] || hiInclusive[i] != hiInclusive[0]) {
                return false;
            }
            if (loLimit[0] == null && loLimit[i] != null || loLimit[0] != null && loLimit[i] == null) {
                return false;
            }
            if (hiLimit[0] == null && hiLimit[i] != null || hiLimit[0] != null && hiLimit[i] == null) {
                return false;
            }
        }
        if (loLimit[0] == null) {
            loInclusive[0] = true;
        }
        if (hiLimit[0] == null) {
            hiInclusive[0] = true;
        }

        ArrayList<Mutable<ILogicalExpression>> keyExprList = new ArrayList<Mutable<ILogicalExpression>>();
        ArrayList<LogicalVariable> keyVarList = new ArrayList<LogicalVariable>();
        ArrayList<Mutable<ILogicalExpression>> rangeSearchFunArgs = new ArrayList<Mutable<ILogicalExpression>>();
        rangeSearchFunArgs.add(new MutableObject<ILogicalExpression>(AccessPathUtils.createStringConstant(chosenIndex.getIndexName())));
        rangeSearchFunArgs.add(new MutableObject<ILogicalExpression>(AccessPathUtils.createStringConstant(FunctionArgumentsConstants.BTREE_INDEX)));
        rangeSearchFunArgs.add(new MutableObject<ILogicalExpression>(AccessPathUtils.createStringConstant(datasetDecl.getName())));

        if (loLimit[0] != null) {
            Mutable<ILogicalExpression> nkRef = new MutableObject<ILogicalExpression>(new ConstantExpression(
                    new AsterixConstantValue(new AInt32(numKeys))));
            rangeSearchFunArgs.add(nkRef);
            for (int i = 0; i < numKeys; i++) {
                LogicalVariable lokVar = context.newVar();
                keyVarList.add(lokVar);
                keyExprList.add(new MutableObject<ILogicalExpression>(new ConstantExpression(loFilter[i])));
                Mutable<ILogicalExpression> loRef = new MutableObject<ILogicalExpression>(new VariableReferenceExpression(
                        lokVar));
                rangeSearchFunArgs.add(loRef);
            }
        } else {
            Mutable<ILogicalExpression> zeroRef = new MutableObject<ILogicalExpression>(new ConstantExpression(
                    new AsterixConstantValue(new AInt32(0))));
            rangeSearchFunArgs.add(zeroRef);
        }

        if (hiLimit[0] != null) {
            Mutable<ILogicalExpression> nkRef = new MutableObject<ILogicalExpression>(new ConstantExpression(
                    new AsterixConstantValue(new AInt32(numKeys))));
            rangeSearchFunArgs.add(nkRef);
            for (int i = 0; i < numKeys; i++) {
                LogicalVariable hikVar = context.newVar();
                keyVarList.add(hikVar);
                keyExprList.add(new MutableObject<ILogicalExpression>(new ConstantExpression(hiFilter[i])));
                Mutable<ILogicalExpression> hiRef = new MutableObject<ILogicalExpression>(new VariableReferenceExpression(
                        hikVar));
                rangeSearchFunArgs.add(hiRef);
            }
        } else {
            Mutable<ILogicalExpression> zeroRef = new MutableObject<ILogicalExpression>(new ConstantExpression(
                    new AsterixConstantValue(new AInt32(0))));
            rangeSearchFunArgs.add(zeroRef);
        }

        ILogicalExpression loExpr = loInclusive[0] ? ConstantExpression.TRUE : ConstantExpression.FALSE;
        rangeSearchFunArgs.add(new MutableObject<ILogicalExpression>(loExpr));
        ILogicalExpression hiExpr = hiInclusive[0] ? ConstantExpression.TRUE : ConstantExpression.FALSE;
        rangeSearchFunArgs.add(new MutableObject<ILogicalExpression>(hiExpr));

        AssignOperator assignSearchKeys = new AssignOperator(keyVarList, keyExprList);
        assignSearchKeys.getInputs().add(dataSourceScan.getInputs().get(0));
        assignSearchKeys.setExecutionMode(dataSourceScan.getExecutionMode());

        IFunctionInfo finfo = FunctionUtils.getFunctionInfo(AsterixBuiltinFunctions.INDEX_SEARCH);
        UnnestingFunctionCallExpression rangeSearchFun = new UnnestingFunctionCallExpression(finfo, rangeSearchFunArgs);
        rangeSearchFun.setReturnsUniqueValues(true);

        List<LogicalVariable> primIdxVarList = dataSourceScan.getVariables();
        int numPrimaryKeys = DatasetUtils.getPartitioningFunctions(datasetDecl).size();

        UnnestMapOperator primIdxUnnestMap;
        AqlMetadataProvider mp = (AqlMetadataProvider) context.getMetadataProvider();
        AqlCompiledMetadataDeclarations metadata = mp.getMetadataDeclarations();

        String itemTypeName = datasetDecl.getItemTypeName();
        ARecordType itemType = (ARecordType) metadata.findType(itemTypeName);
        boolean isPrimaryIndex = chosenIndex == DatasetUtils.getPrimaryIndex(datasetDecl);
        if (!isPrimaryIndex) {
            ArrayList<LogicalVariable> secIdxPrimKeysVarList = new ArrayList<LogicalVariable>(numPrimaryKeys);
            for (int i = 0; i < numPrimaryKeys; i++) {
                secIdxPrimKeysVarList.add(context.newVar());
            }
            ArrayList<LogicalVariable> secIdxUnnestVars = new ArrayList<LogicalVariable>(numKeys
                    + secIdxPrimKeysVarList.size());
            for (int i = 0; i < numKeys; i++) {
                secIdxUnnestVars.add(context.newVar());
            }
            secIdxUnnestVars.addAll(secIdxPrimKeysVarList);
            UnnestMapOperator secIdxUnnest = new UnnestMapOperator(secIdxUnnestVars, new MutableObject<ILogicalExpression>(
                    rangeSearchFun), secondaryIndexTypes(datasetDecl, chosenIndex, itemType));
            secIdxUnnest.getInputs().add(new MutableObject<ILogicalOperator>(assignSearchKeys));
            secIdxUnnest.setExecutionMode(ExecutionMode.PARTITIONED);

            OrderOperator order = new OrderOperator();
            for (LogicalVariable v : secIdxPrimKeysVarList) {
                Mutable<ILogicalExpression> vRef = new MutableObject<ILogicalExpression>(new VariableReferenceExpression(v));
                order.getOrderExpressions().add(
                        new Pair<IOrder, Mutable<ILogicalExpression>>(OrderOperator.ASC_ORDER, vRef));
            }
            order.getInputs().add(new MutableObject<ILogicalOperator>(secIdxUnnest));
            order.setExecutionMode(ExecutionMode.LOCAL);

            List<Mutable<ILogicalExpression>> argList2 = new ArrayList<Mutable<ILogicalExpression>>();
            argList2.add(new MutableObject<ILogicalExpression>(AccessPathUtils.createStringConstant(datasetDecl.getName())));
            argList2.add(new MutableObject<ILogicalExpression>(AccessPathUtils.createStringConstant(FunctionArgumentsConstants.BTREE_INDEX)));
            argList2.add(new MutableObject<ILogicalExpression>(AccessPathUtils.createStringConstant(datasetDecl.getName())));
            argList2.add(new MutableObject<ILogicalExpression>(new ConstantExpression(new AsterixConstantValue(new AInt32(
                    numPrimaryKeys)))));
            for (LogicalVariable v : secIdxPrimKeysVarList) {
                argList2.add(new MutableObject<ILogicalExpression>(new VariableReferenceExpression(v)));
            }
            argList2.add(new MutableObject<ILogicalExpression>(new ConstantExpression(new AsterixConstantValue(new AInt32(
                    numPrimaryKeys)))));
            for (LogicalVariable v : secIdxPrimKeysVarList) {
                argList2.add(new MutableObject<ILogicalExpression>(new VariableReferenceExpression(v)));
            }
            argList2.add(new MutableObject<ILogicalExpression>(ConstantExpression.TRUE));
            argList2.add(new MutableObject<ILogicalExpression>(ConstantExpression.TRUE));
            IFunctionInfo finfoSearch2 = FunctionUtils.getFunctionInfo(AsterixBuiltinFunctions.INDEX_SEARCH);
            AbstractFunctionCallExpression searchPrimIdxFun = new ScalarFunctionCallExpression(finfoSearch2, argList2);
            primIdxUnnestMap = new UnnestMapOperator(primIdxVarList, new MutableObject<ILogicalExpression>(searchPrimIdxFun),
                    AccessPathUtils.primaryIndexTypes(datasetDecl, itemType));
            primIdxUnnestMap.getInputs().add(new MutableObject<ILogicalOperator>(order));
        } else {
            primIdxUnnestMap = new UnnestMapOperator(primIdxVarList, new MutableObject<ILogicalExpression>(rangeSearchFun),
                    AccessPathUtils.primaryIndexTypes(datasetDecl, itemType));
            primIdxUnnestMap.getInputs().add(new MutableObject<ILogicalOperator>(assignSearchKeys));
        }
        primIdxUnnestMap.setExecutionMode(ExecutionMode.PARTITIONED);

        List<Mutable<ILogicalExpression>> remainingFuncExprs = new ArrayList<Mutable<ILogicalExpression>>();
        getNewSelectCondition(select, replacedFuncExprs, remainingFuncExprs);
        if (!remainingFuncExprs.isEmpty()) {
            ILogicalExpression pulledCond = createSelectCondition(remainingFuncExprs);
            SelectOperator selectRest = new SelectOperator(new MutableObject<ILogicalExpression>(pulledCond));            
            if (assign != null) {
                dataSourceScanRef.setValue(primIdxUnnestMap);
                selectRest.getInputs().add(new MutableObject<ILogicalOperator>(assign));
            } else {
                selectRest.getInputs().add(new MutableObject<ILogicalOperator>(primIdxUnnestMap));
            }
            selectRest.setExecutionMode(((AbstractLogicalOperator) selectRef.getValue()).getExecutionMode());
            selectRef.setValue(selectRest);
        } else {
            primIdxUnnestMap.setExecutionMode(ExecutionMode.PARTITIONED);
            if (assign != null) {
                dataSourceScanRef.setValue(primIdxUnnestMap);
                selectRef.setValue(assign);
            } else {
                selectRef.setValue(primIdxUnnestMap);
            }
        }
        return true;
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
            }
            default: {
                throw new IllegalStateException();
            }
        }
        return limit;
    }
    
    private static List<Object> secondaryIndexTypes(AqlCompiledDatasetDecl ddecl, AqlCompiledIndexDecl acid,
            ARecordType itemType) throws AlgebricksException {
        List<Object> types = new ArrayList<Object>();
        for (String sk : acid.getFieldExprs()) {
            types.add(AqlCompiledIndexDecl.keyFieldType(sk, itemType));
        }
        for (Triple<IEvaluatorFactory, ScalarFunctionCallExpression, IAType> t : DatasetUtils
                .getPartitioningFunctions(ddecl)) {
            types.add(t.third);
        }
        return types;
    }
    
    private ILogicalExpression createSelectCondition(List<Mutable<ILogicalExpression>> predList) {
        if (predList.size() > 1) {
            IFunctionInfo finfo = AlgebricksBuiltinFunctions.getBuiltinFunctionInfo(AlgebricksBuiltinFunctions.AND);
            return new ScalarFunctionCallExpression(finfo, predList);
        } else {
            return predList.get(0).getValue();
        }
    }
}

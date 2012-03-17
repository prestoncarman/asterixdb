package edu.uci.ics.asterix.optimizer.rules;

import java.util.ArrayList;
import java.util.List;

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
import edu.uci.ics.asterix.om.util.NonTaggedFormatUtil;
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
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator.ExecutionMode;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.OrderOperator.IOrder;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.utils.Pair;

public class RTreeAccessPath implements IAccessPath {

    private static List<FunctionIdentifier> funcIdents = new ArrayList<FunctionIdentifier>();
    static {
        funcIdents.add(AsterixBuiltinFunctions.SPATIAL_INTERSECT);
    }
    
    public static RTreeAccessPath INSTANCE = new RTreeAccessPath();
    
    @Override
    public List<FunctionIdentifier> getOptimizableFunctions() {
        return funcIdents;
    }

    @Override
    public boolean analyzeFuncExprArgs(AbstractFunctionCallExpression funcExpr, AccessPathAnalysisContext analysisCtx) {
        IAlgebricksConstantValue constFilterVal = null;
        LogicalVariable fieldVar = null;
        ILogicalExpression arg1 = funcExpr.getArguments().get(0).getValue();
        ILogicalExpression arg2 = funcExpr.getArguments().get(1).getValue();        
        // One of the args must be a constant, and the other arg must be a variable.
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
        analysisCtx.matchedFuncExprs.add(new OptimizableFuncExpr(funcExpr, constFilterVal, fieldVar));
        return true;
    }

    @Override
    public boolean matchAllIndexExprs() {
        return true;
    }

    @Override
    public boolean matchPrefixIndexExprs() {
        return false;
    }

    @Override
    public void applyPlanTransformation(Mutable<ILogicalOperator> selectRef, Mutable<ILogicalOperator> assignRef,
            Mutable<ILogicalOperator> dataSourceScanRef, AqlCompiledDatasetDecl datasetDecl, ARecordType recordType,
            AqlCompiledIndexDecl chosenIndex, AccessPathAnalysisContext analysisCtx, IOptimizationContext context)
            throws AlgebricksException {
        // Get the number of dimensions corresponding to the field indexed by
        // chosenIndex.
        IAType spatialType = AqlCompiledIndexDecl.keyFieldType(analysisCtx.matchedFuncExprs.get(0).getFieldName(), recordType);
        int numDimensions = NonTaggedFormatUtil.getNumDimensions(spatialType.getTypeTag());
        int numKeys = numDimensions * 2;
        
        DataSourceScanOperator dataSourceScan = (DataSourceScanOperator) dataSourceScanRef.getValue();

        ArrayList<Mutable<ILogicalExpression>> keyExprList = new ArrayList<Mutable<ILogicalExpression>>();
        ArrayList<LogicalVariable> keyVarList = new ArrayList<LogicalVariable>();
        ArrayList<Mutable<ILogicalExpression>> rangeSearchFunArgs = new ArrayList<Mutable<ILogicalExpression>>();
        rangeSearchFunArgs.add(new MutableObject<ILogicalExpression>(AccessPathUtils.createStringConstant(chosenIndex.getIndexName())));
        rangeSearchFunArgs.add(new MutableObject<ILogicalExpression>(AccessPathUtils.createStringConstant(FunctionArgumentsConstants.RTREE_INDEX)));
        rangeSearchFunArgs.add(new MutableObject<ILogicalExpression>(AccessPathUtils.createStringConstant(datasetDecl.getName())));

        Mutable<ILogicalExpression> nkRef = new MutableObject<ILogicalExpression>(new ConstantExpression(
                new AsterixConstantValue(new AInt32(numKeys))));
        rangeSearchFunArgs.add(nkRef);
        for (int i = 0; i < numKeys; i++) {
            LogicalVariable keyVar = context.newVar();
            keyVarList.add(keyVar);
            AbstractFunctionCallExpression createMBR = new ScalarFunctionCallExpression(
                    FunctionUtils.getFunctionInfo(AsterixBuiltinFunctions.CREATE_MBR));
            createMBR.getArguments().add(new MutableObject<ILogicalExpression>(new ConstantExpression(analysisCtx.matchedFuncExprs.get(0).getConstVal())));
            createMBR.getArguments().add(
                    new MutableObject<ILogicalExpression>(new ConstantExpression(new AsterixConstantValue(
                            new AInt32(numDimensions)))));
            createMBR.getArguments().add(
                    new MutableObject<ILogicalExpression>(new ConstantExpression(new AsterixConstantValue(new AInt32(i)))));
            keyExprList.add(new MutableObject<ILogicalExpression>(createMBR));
            Mutable<ILogicalExpression> keyVarRef = new MutableObject<ILogicalExpression>(new VariableReferenceExpression(
                    keyVar));
            rangeSearchFunArgs.add(keyVarRef);
        }

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
                rangeSearchFun), AccessPathUtils.secondaryIndexTypes(datasetDecl, chosenIndex, itemType, numKeys));
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

        primIdxUnnestMap.setExecutionMode(ExecutionMode.PARTITIONED);
        dataSourceScanRef.setValue(primIdxUnnestMap);
    }
}

package edu.uci.ics.asterix.optimizer.rules;

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
import edu.uci.ics.asterix.om.constants.AsterixConstantValue;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.util.NonTaggedFormatUtil;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
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
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.base.IEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.utils.Pair;
import edu.uci.ics.hyracks.algebricks.core.utils.Triple;

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
        return AccessPathUtils.analyzeFuncExprArgsForOneConstAndVar(funcExpr, analysisCtx);
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
    public boolean applyPlanTransformation(Mutable<ILogicalOperator> selectRef, Mutable<ILogicalOperator> assignRef,
            Mutable<ILogicalOperator> dataSourceScanRef, AqlCompiledDatasetDecl datasetDecl, ARecordType recordType,
            AqlCompiledIndexDecl chosenIndex, AccessPathAnalysisContext analysisCtx, IOptimizationContext context)
            throws AlgebricksException {
        // Get the number of dimensions corresponding to the field indexed by
        // chosenIndex.
        IAType spatialType = AqlCompiledIndexDecl.keyFieldType(analysisCtx.matchedFuncExprs.get(0).getFieldName(), recordType);
        int numDimensions = NonTaggedFormatUtil.getNumDimensions(spatialType.getTypeTag());
        int numKeys = numDimensions * 2;
        
        // TODO: We can probably do something smarter here based on selectivity or MBR area.
        // Pick the first expr optimizable by this index.
        List<Integer> indexExprs = analysisCtx.getIndexExprs(chosenIndex);
        int firstExprIndex = indexExprs.get(0);
        
        DataSourceScanOperator dataSourceScan = (DataSourceScanOperator) dataSourceScanRef.getValue();
        // List of arguments to be passed into an unnest. This unnest will rewritten in the appropriate physical rewrite rule.
        // This logical rewrite rule, and the corresponding physical rewrite have a "contract" as to what does into these arguments. 
        ArrayList<Mutable<ILogicalExpression>> rangeSearchFunArgs = new ArrayList<Mutable<ILogicalExpression>>();
        // Name of chosen index.
        rangeSearchFunArgs.add(new MutableObject<ILogicalExpression>(AccessPathUtils.createStringConstant(chosenIndex.getIndexName())));
        // Type of chosen index.
        rangeSearchFunArgs.add(new MutableObject<ILogicalExpression>(AccessPathUtils.createStringConstant(FunctionArgumentsConstants.RTREE_INDEX)));
        // Name of dataset.
        rangeSearchFunArgs.add(new MutableObject<ILogicalExpression>(AccessPathUtils.createStringConstant(datasetDecl.getName())));
        // Number of keys.
        rangeSearchFunArgs.add(new MutableObject<ILogicalExpression>(new ConstantExpression(new AsterixConstantValue(
                new AInt32(numKeys)))));
        // A spatial object is serialized in the constant of the func expr we are optimizing.
        // The R-Tree expects as input an MBR represented with 1 field per dimension. 
        // Here we generate vars and funcs for extracting MBR fields from the constant into fields of a tuple (as the R-Tree expects them).
        // List of variables for the assign.
        ArrayList<LogicalVariable> keyVarList = new ArrayList<LogicalVariable>();
        // List of expressions for the assign.
        ArrayList<Mutable<ILogicalExpression>> keyExprList = new ArrayList<Mutable<ILogicalExpression>>();
        for (int i = 0; i < numKeys; i++) {
            // The create MBR function "extracts" one field of an MBR around the given spatial object.
            AbstractFunctionCallExpression createMBR = new ScalarFunctionCallExpression(
                    FunctionUtils.getFunctionInfo(AsterixBuiltinFunctions.CREATE_MBR));
            // Spatial object is the constant from the func expr we are optimizing.
            createMBR.getArguments().add(new MutableObject<ILogicalExpression>(new ConstantExpression(analysisCtx.matchedFuncExprs.get(firstExprIndex).getConstVal())));
            // The number of dimensions.
            createMBR.getArguments().add(
                    new MutableObject<ILogicalExpression>(new ConstantExpression(new AsterixConstantValue(
                            new AInt32(numDimensions)))));
            // Which part of the MBR to extract.
            createMBR.getArguments().add(
                    new MutableObject<ILogicalExpression>(new ConstantExpression(new AsterixConstantValue(new AInt32(i)))));
            // Add a variable and its expr to the lists which will be passed into an assign op.
            LogicalVariable keyVar = context.newVar();
            keyVarList.add(keyVar);
            keyExprList.add(new MutableObject<ILogicalExpression>(createMBR));
            // TODO: Still not sure what this one does exactly...
            Mutable<ILogicalExpression> keyVarRef = new MutableObject<ILogicalExpression>(new VariableReferenceExpression(
                    keyVar));
            // Add variable reference to list of arguments for the unnest (which is "consumed" by the physical rewrite rule).
            rangeSearchFunArgs.add(keyVarRef);
        }

        // Assign operator that "extracts" the MBR fields from the func-expr constant into a tuple.
        AssignOperator assignSearchKeys = new AssignOperator(keyVarList, keyExprList);
        // Input to this assign is the EmptyTupleSource (which the dataSourceScan also must have had as input).
        assignSearchKeys.getInputs().add(dataSourceScan.getInputs().get(0));
        assignSearchKeys.setExecutionMode(dataSourceScan.getExecutionMode());

        // This is the logical representation of our RTree search. The actual operator will be plugged in later during physical rewrite.
        // An index search is expressed logically as an unnest over an index-search function.
        IFunctionInfo finfo = FunctionUtils.getFunctionInfo(AsterixBuiltinFunctions.INDEX_SEARCH);
        UnnestingFunctionCallExpression rangeSearchFun = new UnnestingFunctionCallExpression(finfo, rangeSearchFunArgs);
        rangeSearchFun.setReturnsUniqueValues(true);

        List<LogicalVariable> primaryIndexVars = dataSourceScan.getVariables();
        int numPrimaryKeys = DatasetUtils.getPartitioningFunctions(datasetDecl).size();
        
        // List of variables for the primary keys coming out of a secondary-index search.
        ArrayList<LogicalVariable> secondaryIndexPrimaryKeys = new ArrayList<LogicalVariable>(numPrimaryKeys);
        for (int i = 0; i < numPrimaryKeys; i++) {
            secondaryIndexPrimaryKeys.add(context.newVar());
        }
        // List of variables coming out of the secondary-index search. It contains the secondary keys, and the primary keys.
        ArrayList<LogicalVariable> secondaryIndexUnnestVars = new ArrayList<LogicalVariable>(numKeys
                + secondaryIndexPrimaryKeys.size());
        // Add one variable per secondary-index key.
        for (int i = 0; i < numKeys; i++) {
            secondaryIndexUnnestVars.add(context.newVar());
        }
        // Add the primary keys after the secondary keys.
        secondaryIndexUnnestVars.addAll(secondaryIndexPrimaryKeys);
        // This is the operator that the physical rewrite will be looking for. It contains an unnest function that has all necessary arguments to determine
        // which index to use, which variables contain the index-search keys, what is the original dataset, etc.
        UnnestMapOperator secondaryIndexUnnestOp = new UnnestMapOperator(secondaryIndexUnnestVars, new MutableObject<ILogicalExpression>(
                rangeSearchFun), secondaryIndexTypes(datasetDecl, chosenIndex, recordType, numKeys));
        // The unnest op is fed by the op that assigns the search-key fields.
        secondaryIndexUnnestOp.getInputs().add(new MutableObject<ILogicalOperator>(assignSearchKeys));
        secondaryIndexUnnestOp.setExecutionMode(ExecutionMode.PARTITIONED);

        // Add a sort on the primary-index keys before searching the primary index.
        OrderOperator order = new OrderOperator();
        for (LogicalVariable var : secondaryIndexPrimaryKeys) {
            Mutable<ILogicalExpression> varRef = new MutableObject<ILogicalExpression>(new VariableReferenceExpression(var));
            order.getOrderExpressions().add(
                    new Pair<IOrder, Mutable<ILogicalExpression>>(OrderOperator.ASC_ORDER, varRef));
        }
        // The secondary-index search feeds into the sort.
        order.getInputs().add(new MutableObject<ILogicalOperator>(secondaryIndexUnnestOp));
        order.setExecutionMode(ExecutionMode.LOCAL);
        
        // Prepare 
        List<Mutable<ILogicalExpression>> argList2 = new ArrayList<Mutable<ILogicalExpression>>();
        argList2.add(new MutableObject<ILogicalExpression>(AccessPathUtils.createStringConstant(datasetDecl.getName())));
        argList2.add(new MutableObject<ILogicalExpression>(AccessPathUtils.createStringConstant(FunctionArgumentsConstants.BTREE_INDEX)));
        argList2.add(new MutableObject<ILogicalExpression>(AccessPathUtils.createStringConstant(datasetDecl.getName())));
        argList2.add(new MutableObject<ILogicalExpression>(new ConstantExpression(new AsterixConstantValue(new AInt32(
                numPrimaryKeys)))));
        for (LogicalVariable v : secondaryIndexPrimaryKeys) {
            argList2.add(new MutableObject<ILogicalExpression>(new VariableReferenceExpression(v)));
        }
        argList2.add(new MutableObject<ILogicalExpression>(new ConstantExpression(new AsterixConstantValue(new AInt32(
                numPrimaryKeys)))));
        for (LogicalVariable v : secondaryIndexPrimaryKeys) {
            argList2.add(new MutableObject<ILogicalExpression>(new VariableReferenceExpression(v)));
        }
        argList2.add(new MutableObject<ILogicalExpression>(ConstantExpression.TRUE));
        argList2.add(new MutableObject<ILogicalExpression>(ConstantExpression.TRUE));
        IFunctionInfo finfoSearch2 = FunctionUtils.getFunctionInfo(AsterixBuiltinFunctions.INDEX_SEARCH);
        AbstractFunctionCallExpression searchPrimIdxFun = new ScalarFunctionCallExpression(finfoSearch2, argList2);
        UnnestMapOperator primaryIndexUnnestMap = new UnnestMapOperator(primaryIndexVars, new MutableObject<ILogicalExpression>(searchPrimIdxFun),
                AccessPathUtils.primaryIndexTypes(datasetDecl, recordType));
        primaryIndexUnnestMap.getInputs().add(new MutableObject<ILogicalOperator>(order));

        primaryIndexUnnestMap.setExecutionMode(ExecutionMode.PARTITIONED);
        dataSourceScanRef.setValue(primaryIndexUnnestMap);
        
        return true;
    }
    
    private static List<Object> secondaryIndexTypes(AqlCompiledDatasetDecl datasetDecl, AqlCompiledIndexDecl index,
            ARecordType itemType, int numKeys) throws AlgebricksException {
        List<Object> types = new ArrayList<Object>();
        IAType keyType = AqlCompiledIndexDecl.keyFieldType(index.getFieldExprs().get(0), itemType);
        IAType nestedKeyType = NonTaggedFormatUtil.getNestedSpatialType(keyType.getTypeTag());
        for (int i = 0; i < numKeys; i++) {
            types.add(nestedKeyType);
        }
        for (Triple<IEvaluatorFactory, ScalarFunctionCallExpression, IAType> t : DatasetUtils
                .getPartitioningFunctions(datasetDecl)) {
            types.add(t.third);
        }
        return types;
    }
}

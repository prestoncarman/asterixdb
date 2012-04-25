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
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.base.IEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.utils.Triple;

public class RTreeAccessMethod implements IAccessMethod {

    private static List<FunctionIdentifier> funcIdents = new ArrayList<FunctionIdentifier>();
    static {
        funcIdents.add(AsterixBuiltinFunctions.SPATIAL_INTERSECT);
    }
    
    public static RTreeAccessMethod INSTANCE = new RTreeAccessMethod();
    
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
        return false;
    }

    @Override
    public boolean applyPlanTransformation(Mutable<ILogicalOperator> selectRef, Mutable<ILogicalOperator> assignRef,
            Mutable<ILogicalOperator> dataSourceScanRef, AqlCompiledDatasetDecl datasetDecl, ARecordType recordType,
            AqlCompiledIndexDecl chosenIndex, AccessMethodAnalysisContext analysisCtx, IOptimizationContext context)
            throws AlgebricksException {
        // TODO: We can probably do something smarter here based on selectivity or MBR area.
        // Pick the first expr optimizable by this index.
        List<Integer> indexExprs = analysisCtx.getIndexExprs(chosenIndex);
        int firstExprIndex = indexExprs.get(0);
        IOptimizableFuncExpr optFuncExpr = analysisCtx.matchedFuncExprs.get(firstExprIndex);
        
        // Get the number of dimensions corresponding to the field indexed by
        // chosenIndex.
        IAType spatialType = AqlCompiledIndexDecl.keyFieldType(optFuncExpr.getFieldName(0), recordType);
        int numDimensions = NonTaggedFormatUtil.getNumDimensions(spatialType.getTypeTag());
        int numSecondaryKeys = numDimensions * 2;
        
        DataSourceScanOperator dataSourceScan = (DataSourceScanOperator) dataSourceScanRef.getValue();
        // List of arguments to be passed into an unnest.
        // This logical rewrite rule, and the corresponding runtime op generated in the jobgen 
        // have a contract as to what goes into these arguments.
        // Here, we put the name of the chosen index, the type of index, the name of the dataset, 
        // the number of secondary-index keys, and the variable references corresponding to the secondary-index search keys.
        ArrayList<Mutable<ILogicalExpression>> secondaryIndexFuncArgs = new ArrayList<Mutable<ILogicalExpression>>();
        secondaryIndexFuncArgs.add(new MutableObject<ILogicalExpression>(AccessMethodUtils.createStringConstant(chosenIndex.getIndexName())));
        secondaryIndexFuncArgs.add(new MutableObject<ILogicalExpression>(AccessMethodUtils.createStringConstant(FunctionArgumentsConstants.RTREE_INDEX)));
        secondaryIndexFuncArgs.add(new MutableObject<ILogicalExpression>(AccessMethodUtils.createStringConstant(datasetDecl.getName())));
        secondaryIndexFuncArgs.add(new MutableObject<ILogicalExpression>(new ConstantExpression(new AsterixConstantValue(
                new AInt32(numSecondaryKeys)))));
        // A spatial object is serialized in the constant of the func expr we are optimizing.
        // The R-Tree expects as input an MBR represented with 1 field per dimension. 
        // Here we generate vars and funcs for extracting MBR fields from the constant into fields of a tuple (as the R-Tree expects them).
        // List of variables for the assign.
        ArrayList<LogicalVariable> keyVarList = new ArrayList<LogicalVariable>();
        // List of expressions for the assign.
        ArrayList<Mutable<ILogicalExpression>> keyExprList = new ArrayList<Mutable<ILogicalExpression>>();
        for (int i = 0; i < numSecondaryKeys; i++) {
            // The create MBR function "extracts" one field of an MBR around the given spatial object.
            AbstractFunctionCallExpression createMBR = new ScalarFunctionCallExpression(
                    FunctionUtils.getFunctionInfo(AsterixBuiltinFunctions.CREATE_MBR));
            // Spatial object is the constant from the func expr we are optimizing.
            createMBR.getArguments().add(new MutableObject<ILogicalExpression>(new ConstantExpression(optFuncExpr.getConstantVal(0))));
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
            // Add variable reference to list of arguments for the unnest (which is "consumed" by jobgen).
            Mutable<ILogicalExpression> keyVarRef = new MutableObject<ILogicalExpression>(new VariableReferenceExpression(
                    keyVar));
            secondaryIndexFuncArgs.add(keyVarRef);
        }

        // Assign operator that "extracts" the MBR fields from the func-expr constant into a tuple.
        AssignOperator assignSearchKeys = new AssignOperator(keyVarList, keyExprList);
        // Input to this assign is the EmptyTupleSource (which the dataSourceScan also must have had as input).
        assignSearchKeys.getInputs().add(dataSourceScan.getInputs().get(0));
        assignSearchKeys.setExecutionMode(dataSourceScan.getExecutionMode());

        // This is the logical representation of our RTree search.
        // An index search is expressed logically as an unnest over an index-search function.
        IFunctionInfo secondaryIndexSearch = FunctionUtils.getFunctionInfo(AsterixBuiltinFunctions.INDEX_SEARCH);
        UnnestingFunctionCallExpression rangeSearchFun = new UnnestingFunctionCallExpression(secondaryIndexSearch, secondaryIndexFuncArgs);
        rangeSearchFun.setReturnsUniqueValues(true);

        // Generate the rest of the upstream plan which feeds the search results into the primary index.
        List<LogicalVariable> primaryIndexVars = dataSourceScan.getVariables();
        List<Object> secondaryIndexTypes = getSecondaryIndexTypes(datasetDecl, chosenIndex, recordType);
        UnnestMapOperator primaryIndexUnnestMap = AccessMethodUtils.createPrimaryIndexUnnestMap(datasetDecl, recordType,
                primaryIndexVars, chosenIndex, numSecondaryKeys, secondaryIndexTypes, rangeSearchFun, assignSearchKeys,
                context, false, true);
        // Replace the datasource scan with the new plan rooted at primaryIndexUnnestMap.
        dataSourceScanRef.setValue(primaryIndexUnnestMap);
        return true;
    }
    
    /**
     * @return A list of types corresponding to fields produced by the given
     *         index when searched.
     */
    private static List<Object> getSecondaryIndexTypes(AqlCompiledDatasetDecl datasetDecl, AqlCompiledIndexDecl index,
            ARecordType itemType) throws AlgebricksException {
        List<Object> types = new ArrayList<Object>();
        IAType keyType = AqlCompiledIndexDecl.keyFieldType(index.getFieldExprs().get(0), itemType);
        int numDimensions = NonTaggedFormatUtil.getNumDimensions(keyType.getTypeTag());
        int numKeys = numDimensions * 2;
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

    @Override
    public boolean exprIsOptimizable(AqlCompiledIndexDecl index, IOptimizableFuncExpr optFuncExpr) {
        // No additional analysis required.
        return true;
    }
}

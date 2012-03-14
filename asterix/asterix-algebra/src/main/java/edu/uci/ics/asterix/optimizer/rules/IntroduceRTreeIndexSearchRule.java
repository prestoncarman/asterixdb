package edu.uci.ics.asterix.optimizer.rules;

import java.util.ArrayList;
import java.util.HashMap;
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
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.base.IEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.utils.Pair;
import edu.uci.ics.hyracks.algebricks.core.utils.Triple;

public class IntroduceRTreeIndexSearchRule extends IntroduceTreeIndexSearchRule {

    @Override
    protected boolean matchesConcreteRule(FunctionIdentifier funcIdent) {
        return AsterixBuiltinFunctions.isSpatialFilterFunction(funcIdent);
    }
    
    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        // Match operator pattern and initialize operator members.
        if (!matchesPattern(opRef, context, false)) {
            return false;
        }

        // List of constant values participating in spatial filter that we can
		// push to the index. Used to generate the input tuple an index lookup.
		ArrayList<IAlgebricksConstantValue> outFilters = new ArrayList<IAlgebricksConstantValue>();
		// List of variables participating in a spatial filter that we can push
		// to the index. Used to consult the metadata whether there is an applicable index.
		ArrayList<LogicalVariable> outComparedVars = new ArrayList<LogicalVariable>();
		// Analyzes the select condition, filling outFilters and outComparedVars.
		if (!analyzeCondition(selectCond, outFilters, outComparedVars)) {
			return false;
		}
		
		// Set dataset and type metadata.
		if (!setDatasetAndTypeMetadata((AqlMetadataProvider)context.getMetadataProvider())) {
		    return false;
		}
       
		// Contains candidate indexes and corresponding expressions that we could optimize. 
        // Maps from index to a list of pairs. Each list-entry is a <fieldName,varPos> pair. 
        // The varPos is an index into outComparedVars, and fieldName is the corresponding fieldName.
        HashMap<AqlCompiledIndexDecl, List<Pair<String, Integer>>> indexExprs = new HashMap<AqlCompiledIndexDecl, List<Pair<String, Integer>>>();
        List<LogicalVariable> varList = assign.getVariables();
        fillAllIndexExprs(varList, outComparedVars, indexExprs);
        // We didn't find any applicable indexes.
        if (indexExprs.isEmpty()) {
        	return false;
        }
        
        AqlCompiledIndexDecl chosenIndex = chooseIndex(datasetDecl, indexExprs);
        if (chosenIndex == null) {
        	context.addToDontApplySet(this, select);
        	return false;
        }
        List<Pair<String, Integer>> psiList = indexExprs.get(chosenIndex);
        // Get the number of dimensions corresponding to the field indexed by chosenIndex.
        IAType spatialType = AqlCompiledIndexDecl.keyFieldType(psiList.get(0).first, recordType);
        int numDimensions = NonTaggedFormatUtil.getNumDimensions(spatialType.getTypeTag());
        applyPlanTransformation(dataSourceScanRef, dataSourceScan, assign, outFilters, datasetDecl, chosenIndex,
        		context, numDimensions);
        OperatorPropertiesUtil.typeOpRec(opRef, context);
        context.addToDontApplySet(this, select);
        return true;
    }

	/**
	 * Analyzes the condition of a select to see if it contains a spatial filter
	 * function. Assumes the caller has already checked that cond is a function
	 * call.
	 * 
	 * @param cond
	 *            Input select condition.
	 * 
	 * @param outFilters
	 *            Output parameter. List of constant values participating in
	 *            spatial filter that we can push to an index. Typically will
	 *            have a single element.
	 * 
	 * @param outComparedVars
	 *            Output parameter. List of participating variables in a spatial
	 *            filter than we can push to an index. Typically will have a
	 *            single element.
	 * 
	 * @return boolean
	 * 
	 */
    private boolean analyzeCondition(ILogicalExpression cond, List<IAlgebricksConstantValue> outFilters,
    		List<LogicalVariable> outComparedVars) {
    	AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) cond;
    	FunctionIdentifier funcIdent = funcExpr.getFunctionIdentifier();
    	// Check whether the condition is a single spatial filter.
    	if (AsterixBuiltinFunctions.isSpatialFilterFunction(funcIdent)) {
    		return analyzeSpatialFilterExpr(funcExpr, outFilters, outComparedVars);
    	}
    	// Walk through the function's arguments to find other functions.
    	boolean found = false;
    	for (Mutable<ILogicalExpression> arg : funcExpr.getArguments()) {
    		ILogicalExpression argExpr = arg.getValue();
    		if (argExpr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
    			continue;
    		}
    		AbstractFunctionCallExpression argFuncExpr = (AbstractFunctionCallExpression) argExpr;
    		if (AsterixBuiltinFunctions.isSpatialFilterFunction(argFuncExpr.getFunctionIdentifier())) {
    			if (analyzeSpatialFilterExpr(argFuncExpr, outFilters, outComparedVars)) {
    				found = true;
    			}
    		}
    	}
    	return found;
    }

	/**
	 * Analyzes the given spatial filter function call expression funcExpr.
	 * Assumes the caller has already established that funcExpr is a spatial
	 * filter function.
	 * 
	 * @param funcExpr
	 *            Input spatial filter function call expression.
	 * 
	 * @param outFilters
	 *            Output parameter. List of constant values participating in
	 *            spatial filter that we can push to an index. Typically will
	 *            have a single element.
	 * 
	 * @param outComparedVars
	 *            Output parameter. List of participating variables in a spatial
	 *            filter than we can push to an index. Typically will have a
	 *            single element.
	 * 
	 * @return boolean
	 * 
	 */
    private boolean analyzeSpatialFilterExpr(AbstractFunctionCallExpression funcExpr,
            List<IAlgebricksConstantValue> outFilters, List<LogicalVariable> outComparedVars) {
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
        outFilters.add(constFilterVal);
        outComparedVars.add(fieldVar);
        return true;
    }

    private void applyPlanTransformation(Mutable<ILogicalOperator> opRef3, DataSourceScanOperator scanDataset,
            AssignOperator assignFieldAccess, ArrayList<IAlgebricksConstantValue> filters,
            AqlCompiledDatasetDecl ddecl, AqlCompiledIndexDecl picked, 
            IOptimizationContext context, int dimension) throws AlgebricksException {
        int numKeys = dimension * 2;

        ArrayList<Mutable<ILogicalExpression>> keyExprList = new ArrayList<Mutable<ILogicalExpression>>();
        ArrayList<LogicalVariable> keyVarList = new ArrayList<LogicalVariable>();
        ArrayList<Mutable<ILogicalExpression>> rangeSearchFunArgs = new ArrayList<Mutable<ILogicalExpression>>();
        rangeSearchFunArgs.add(new MutableObject<ILogicalExpression>(createStringConstant(picked.getIndexName())));
        rangeSearchFunArgs.add(new MutableObject<ILogicalExpression>(createStringConstant(FunctionArgumentsConstants.RTREE_INDEX)));
        rangeSearchFunArgs.add(new MutableObject<ILogicalExpression>(createStringConstant(ddecl.getName())));

        Mutable<ILogicalExpression> nkRef = new MutableObject<ILogicalExpression>(new ConstantExpression(
                new AsterixConstantValue(new AInt32(numKeys))));
        rangeSearchFunArgs.add(nkRef);
        for (int i = 0; i < numKeys; i++) {
            LogicalVariable keyVar = context.newVar();
            keyVarList.add(keyVar);

            AbstractFunctionCallExpression createMBR = new ScalarFunctionCallExpression(
                    FunctionUtils.getFunctionInfo(AsterixBuiltinFunctions.CREATE_MBR));
            createMBR.getArguments().add(new MutableObject<ILogicalExpression>(new ConstantExpression(filters.get(0))));
            createMBR.getArguments().add(
                    new MutableObject<ILogicalExpression>(new ConstantExpression(new AsterixConstantValue(
                            new AInt32(dimension)))));
            createMBR.getArguments().add(
                    new MutableObject<ILogicalExpression>(new ConstantExpression(new AsterixConstantValue(new AInt32(i)))));
            keyExprList.add(new MutableObject<ILogicalExpression>(createMBR));
            Mutable<ILogicalExpression> keyVarRef = new MutableObject<ILogicalExpression>(new VariableReferenceExpression(
                    keyVar));
            rangeSearchFunArgs.add(keyVarRef);
        }

        AssignOperator assignSearchKeys = new AssignOperator(keyVarList, keyExprList);
        assignSearchKeys.getInputs().add(scanDataset.getInputs().get(0));
        assignSearchKeys.setExecutionMode(scanDataset.getExecutionMode());

        IFunctionInfo finfo = FunctionUtils.getFunctionInfo(AsterixBuiltinFunctions.INDEX_SEARCH);
        UnnestingFunctionCallExpression rangeSearchFun = new UnnestingFunctionCallExpression(finfo, rangeSearchFunArgs);
        rangeSearchFun.setReturnsUniqueValues(true);

        List<LogicalVariable> primIdxVarList = scanDataset.getVariables();
        int numPrimaryKeys = DatasetUtils.getPartitioningFunctions(ddecl).size();

        UnnestMapOperator primIdxUnnestMap;
        AqlMetadataProvider mp = (AqlMetadataProvider) context.getMetadataProvider();
        AqlCompiledMetadataDeclarations metadata = mp.getMetadataDeclarations();

        String itemTypeName = ddecl.getItemTypeName();
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
        		rangeSearchFun), secondaryIndexTypes(ddecl, picked, itemType, numKeys));
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
        argList2.add(new MutableObject<ILogicalExpression>(createStringConstant(ddecl.getName())));
        argList2.add(new MutableObject<ILogicalExpression>(createStringConstant(FunctionArgumentsConstants.BTREE_INDEX)));
        argList2.add(new MutableObject<ILogicalExpression>(createStringConstant(ddecl.getName())));
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
        		primaryIndexTypes(metadata, ddecl, itemType));
        primIdxUnnestMap.getInputs().add(new MutableObject<ILogicalOperator>(order));

        primIdxUnnestMap.setExecutionMode(ExecutionMode.PARTITIONED);
        opRef3.setValue(primIdxUnnestMap);
    }

    private static List<Object> secondaryIndexTypes(AqlCompiledDatasetDecl ddecl, AqlCompiledIndexDecl acid,
            ARecordType itemType, int numKeys) throws AlgebricksException {
        List<Object> types = new ArrayList<Object>();
        IAType keyType = AqlCompiledIndexDecl.keyFieldType(acid.getFieldExprs().get(0), itemType);
        IAType nestedKeyType = NonTaggedFormatUtil.getNestedSpatialType(keyType.getTypeTag());

        for (int i = 0; i < numKeys; i++) {
            types.add(nestedKeyType);
        }
        for (Triple<IEvaluatorFactory, ScalarFunctionCallExpression, IAType> t : DatasetUtils
                .getPartitioningFunctions(ddecl)) {
            types.add(t.third);
        }
        return types;
    }
}

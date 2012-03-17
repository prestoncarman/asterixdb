package edu.uci.ics.asterix.optimizer.rules;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.asterix.common.config.DatasetConfig.DatasetType;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledDatasetDecl;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledIndexDecl;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledMetadataDeclarations;
import edu.uci.ics.asterix.metadata.declared.AqlMetadataProvider;
import edu.uci.ics.asterix.metadata.utils.DatasetUtils;
import edu.uci.ics.asterix.om.base.AInt32;
import edu.uci.ics.asterix.om.base.AString;
import edu.uci.ics.asterix.om.constants.AsterixConstantValue;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.optimizer.base.AnalysisUtil;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractLogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import edu.uci.ics.hyracks.algebricks.core.utils.Pair;
import edu.uci.ics.hyracks.algebricks.core.utils.Triple;

/**
 * This rule tries to optimize simple selections with indexes.
 * 
 * Matches this operator pattern: (select) <-- (assign) <-- (datasource scan)
 * Replaces it with this pattern: (select) <-- (assign) <-- (btree search) <-- (index search)
 * 
 * For the special case of only primary index lookups it may also match the following pattern:
 * (select) <-- (datasource scan)
 * since no assign is necessary to get the primary key fields (they are already stored fields in the BTree tuples).
 * 
 * Note that for some index-based optimizations do not remove the triggering condition from the select, 
 * since the index only acts as a filter, and the final verification must still be done 
 * via the original function.
 * 
 * The basic outline of this rule is:
 * 1. Match operator pattern.
 * 2. Analyze select to see if there are optimizable functions.
 * 3. Check metadata to see if there are applicable indexex.
 * 4. Choose an index to apply.
 * 4. Rewrite plan using index.
 * 
 */
// TODO: Rename this to IntroduceIndexSearchRule because secondary inverted indexes may also apply.
public class IntroduceTreeIndexSearchRule implements IAlgebraicRewriteRule {

    
	// Operators representing the pattern to be matched:
	// (select) <-- (assign) <-- (datasource scan)
    // OR
	// (select) <-- (datasource scan)
    // These ops are set in matchesPattern()
	protected SelectOperator select = null;
	protected AssignOperator assign = null;
	protected DataSourceScanOperator dataSourceScan = null;
	// Original operator refs corresponding to the ops above.
	protected Mutable<ILogicalOperator> selectRef = null;
	protected Mutable<ILogicalOperator> assignRef = null;
	protected Mutable<ILogicalOperator> dataSourceScanRef = null;
	// Condition of the select operator.
	protected AbstractFunctionCallExpression selectCond = null;
	protected boolean includePrimaryIndex = true;
	// Dataaset and type metadata. Set in setDatasetAndTypeMetadata().
	protected ARecordType recordType = null;
	protected AqlCompiledDatasetDecl datasetDecl = null;
	
	protected static Map<FunctionIdentifier, List<IAccessPath>> accessPaths = new HashMap<FunctionIdentifier, List<IAccessPath>>();
	static {
	    registerAccessPath(RTreeAccessPath.INSTANCE);
	}
	
	public static void registerAccessPath(IAccessPath accessPath) {
	    List<FunctionIdentifier> funcs = accessPath.getOptimizableFunctions();
	    for (FunctionIdentifier funcIdent : funcs) {
	        List<IAccessPath> l = accessPaths.get(funcIdent);
	        if (l == null) {
	            l = new ArrayList<IAccessPath>();
	            accessPaths.put(funcIdent, l);
	        }
	        l.add(accessPath);
	    }
	}
	
    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        return false;
    }
    
    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        // Match operator pattern and initialize operator members.
        if (!matchesOperatorPattern(opRef, context, true)) {
            return false;
        }
        
        // Analyze select condition.
        Map<IAccessPath, AccessPathAnalysisContext> analyzedAccPaths = new HashMap<IAccessPath, AccessPathAnalysisContext>();
        if (!analyzeSelectCondition(selectCond, analyzedAccPaths)) {
            return false;
        }

        // Set dataset and type metadata.
        if (!setDatasetAndTypeMetadata((AqlMetadataProvider)context.getMetadataProvider())) {
            return false;
        }
        
        // The assign may be null if there is only a filter on the primary index key.
        List<LogicalVariable> varList = (assign != null) ? assign.getVariables() : dataSourceScan.getVariables();
        Iterator<Map.Entry<IAccessPath, AccessPathAnalysisContext>> accPathIt = analyzedAccPaths.entrySet().iterator();
        while (accPathIt.hasNext()) {
            Map.Entry<IAccessPath, AccessPathAnalysisContext> entry = accPathIt.next();
            AccessPathAnalysisContext accPathCtx = entry.getValue();
            fillAllIndexExprs(varList, accPathCtx);
            pruneIndexCandidates(entry.getKey(), accPathCtx);
            // Remove access paths for which there are definitely no applicable indexes.
            if (entry.getValue().indexExprs.isEmpty()) {
                accPathIt.remove();
            }
        }
        
        // Choose index to be applied.
        Pair<IAccessPath, AqlCompiledIndexDecl> chosenIndex = chooseIndex(analyzedAccPaths);
        if (chosenIndex == null) {
            context.addToDontApplySet(this, select);
            return false;
        }
        
        // Apply plan transformation using chosen index.
        AccessPathAnalysisContext analysisCtx = analyzedAccPaths.get(chosenIndex.first);
        chosenIndex.first.applyPlanTransformation(selectRef, assignRef, dataSourceScanRef, datasetDecl, recordType,
                chosenIndex.second, analysisCtx, context);
        
        System.out.println("HUHU");
        
        OperatorPropertiesUtil.typeOpRec(opRef, context);
        context.addToDontApplySet(this, select);
        
        return false;
    }
    
    /**
     * Simply picks the first index that it finds.
     * TODO: Improve this decision process by making it more systematic.
     * 
     * @param datasetDecl
     * @param indexExprs
     * @return
     */
    protected Pair<IAccessPath, AqlCompiledIndexDecl> chooseIndex(
            Map<IAccessPath, AccessPathAnalysisContext> analyzedAccPaths) {
        Iterator<Map.Entry<IAccessPath, AccessPathAnalysisContext>> accPathIt = analyzedAccPaths.entrySet().iterator();
        while (accPathIt.hasNext()) {
            Map.Entry<IAccessPath, AccessPathAnalysisContext> accPathEntry = accPathIt.next();
            AccessPathAnalysisContext analysisCtx = accPathEntry.getValue();
            Iterator<Map.Entry<AqlCompiledIndexDecl, List<Integer>>> indexIt = analysisCtx.indexExprs.entrySet().iterator();
            if (indexIt.hasNext()) {
                Map.Entry<AqlCompiledIndexDecl, List<Integer>> indexEntry = indexIt.next();
                return new Pair<IAccessPath, AqlCompiledIndexDecl>(accPathEntry.getKey(), indexEntry.getKey());
            }
        }
        return null;
    }
    
    /**
     * Removes irrelevant access path candidates, based on whether the
     * expressions in the query match those in the index. For example, some
     * index may require all its expressions to be matched, and some indexes may
     * only require a match on a prefix of fields to be applicable. This methods
     * removes all index candidates indexExprs that are definitely not
     * applicable according to the expressions involved.
     * 
     */
    public void pruneIndexCandidates(IAccessPath accessPath, AccessPathAnalysisContext analysisCtx) {
        Iterator<Map.Entry<AqlCompiledIndexDecl, List<Integer>>> it = analysisCtx.indexExprs.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<AqlCompiledIndexDecl, List<Integer>> entry = it.next();
            AqlCompiledIndexDecl index = entry.getKey();
            List<Integer> exprs = entry.getValue();
            boolean allUsed = true;
            int lastFieldMatched = -1;
            for (int i = 0; i < index.getFieldExprs().size(); i++) {
                String keyField = index.getFieldExprs().get(i);
                boolean foundKeyField = false;
                for (Integer ix : exprs) {
                    if (analysisCtx.matchedFuncExprs.get(ix).getFieldName().equals(keyField)) {
                        foundKeyField = true;
                        if (lastFieldMatched == i - 1) {
                            lastFieldMatched = i;
                        }
                        break;
                    }
                }
                if (!foundKeyField) {
                    allUsed = false;
                    break;
                }                
            }
            // If the access path requires all exprs to be matched but they are not, remove this candidate.
            if (!allUsed && accessPath.matchAllIndexExprs()) {
                it.remove();
                return;
            }
            // A prefix of the index exprs may have been matched.
            if (lastFieldMatched < 0 && accessPath.matchPrefixIndexExprs()) {
                it.remove();
                return;
            }
        }
    }
    
    /**
     * Analyzes the given selection condition, filling analyzedAccPaths with applicable access path types.
     * At this point we are not yet consulting the metadata whether an actual index exists or not.
     * 
     * @param cond
     * @param analyzedAccPaths
     * @return
     */
    protected boolean analyzeSelectCondition(ILogicalExpression cond, Map<IAccessPath, AccessPathAnalysisContext> analyzedAccPaths) {
        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) cond;
        boolean found = analyzeFunctionExpr(funcExpr, analyzedAccPaths);        
        for (Mutable<ILogicalExpression> arg : funcExpr.getArguments()) {
            ILogicalExpression argExpr = arg.getValue();
            if (argExpr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
                continue;
            }
            AbstractFunctionCallExpression argFuncExpr = (AbstractFunctionCallExpression) argExpr;
            found = found || analyzeFunctionExpr(argFuncExpr, analyzedAccPaths);
        }
        return found;
    }
    
    /**
     * Finds applicable access paths for the given function expression based on
     * the function identifier, and an analysis of the function's arguments.
     * Updates the analyzedAccPaths accordingly.
     * 
     * @param cond
     * @param analyzedAccPaths
     * @return
     */
    protected boolean analyzeFunctionExpr(AbstractFunctionCallExpression funcExpr, Map<IAccessPath, AccessPathAnalysisContext> analyzedAccPaths) {
        FunctionIdentifier funcIdent = funcExpr.getFunctionIdentifier();        
        if (funcIdent == AlgebricksBuiltinFunctions.AND) {
            return false;
        }
        // Retrieves the list of access paths that are relevant based on the funcIdent.
        List<IAccessPath> relevantAccessPaths = accessPaths.get(funcIdent);
        if (relevantAccessPaths == null) {
            return false;
        }
        boolean atLeastOneMatchFound = false;
        // Placeholder for a new analysis context in case we need one.
        AccessPathAnalysisContext newAnalysisCtx = new AccessPathAnalysisContext();
        for(IAccessPath accessPath : relevantAccessPaths) {
            AccessPathAnalysisContext analysisCtx = analyzedAccPaths.get(accessPath);
            // Use the current placeholder.
            if (analysisCtx == null) {
                analysisCtx = newAnalysisCtx;
            }
            // Analyzes the funcExpr's arguments to see if the accessPath is truly applicable.
            boolean matchFound = accessPath.analyzeFuncExprArgs(funcExpr, analysisCtx);
            if (matchFound) {
                // If we've used the current new context placeholder, replace it with a new one.
                if (analysisCtx == newAnalysisCtx) {
                    analyzedAccPaths.put(accessPath, analysisCtx);
                    newAnalysisCtx = new AccessPathAnalysisContext();
                }
                atLeastOneMatchFound = true;
            }
        }
        return atLeastOneMatchFound;
    }
    
    protected boolean matchesConcreteRule(FunctionIdentifier funcIdent) {
        return false;
    }
    
    protected boolean matchesOperatorPattern(Mutable<ILogicalOperator> opRef, IOptimizationContext context, boolean includePrimaryIndex) {
        this.includePrimaryIndex = includePrimaryIndex;
        // First check that the operator is a select and its condition is a function call.
        AbstractLogicalOperator op1 = (AbstractLogicalOperator) opRef.getValue();
        if (context.checkIfInDontApplySet(this, op1)) {
            return false;
        }
        // Check op is a select.
        if (op1.getOperatorTag() != LogicalOperatorTag.SELECT) {
            return false;
        }
        // Set and analyze select.
        selectRef = opRef;
        select = (SelectOperator) op1;
        ILogicalExpression condExpr = select.getCondition().getValue();
        if (condExpr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }
        // Check that the select's condition is a function call.
        selectCond = (AbstractFunctionCallExpression) condExpr;
        // Examine the select's children to match the expected pattern:
        // (select) <-- (assign) <-- (datasource scan)
        // OR
        // (select) <-- (datasource scan)
        // Match assign.
        Mutable<ILogicalOperator> opRef2 = op1.getInputs().get(0);
        AbstractLogicalOperator op2 = (AbstractLogicalOperator) opRef2.getValue();
        // Match assign.
        if (op2.getOperatorTag() != LogicalOperatorTag.ASSIGN) {
            // Pattern may still match if we are looking for primary index matches as well.
            if (includePrimaryIndex && op2.getOperatorTag() == LogicalOperatorTag.DATASOURCESCAN) {
                dataSourceScanRef = opRef2;
                dataSourceScan = (DataSourceScanOperator) op2;
                return true;
            }
            return false;
        }
        assignRef = opRef2;
        assign = (AssignOperator) op2;
        // Match datasource scan.
        Mutable<ILogicalOperator> opRef3 = op2.getInputs().get(0);
        AbstractLogicalOperator op3 = (AbstractLogicalOperator) opRef3.getValue();
        if (op3.getOperatorTag() != LogicalOperatorTag.DATASOURCESCAN) {
            return false;
        }
        dataSourceScanRef = opRef3;
        dataSourceScan = (DataSourceScanOperator) op3;
        return true;
    }

    /**
     * Find the dataset corresponding to the datasource scan in the metadata.
     * Also sets recordType to be the type of that dataset.
     */  
    protected boolean setDatasetAndTypeMetadata(AqlMetadataProvider metadataProvider) throws AlgebricksException {
        // Find the dataset corresponding to the datasource scan in the metadata.
        String datasetName = AnalysisUtil.getDatasetName(dataSourceScan);
        if (datasetName == null) {
            return false;
        }
        AqlCompiledMetadataDeclarations metadata = metadataProvider.getMetadataDeclarations();
        datasetDecl = metadata.findDataset(datasetName);
        if (datasetDecl == null) {
            throw new AlgebricksException("No metadata for dataset " + datasetName);
        }
        if (datasetDecl.getDatasetType() != DatasetType.INTERNAL && datasetDecl.getDatasetType() != DatasetType.FEED) {
            return false;
        }
        // Get the record type for that dataset.
        IAType itemType = metadata.findType(datasetDecl.getItemTypeName());
        if (itemType.getTypeTag() != ATypeTag.RECORD) {
            return false;
        }
        recordType = (ARecordType) itemType;
        return true;
    }
       
	/**
	 * 
	 * @return returns true if a candidate index was added to foundIndexExprs,
	 *         false otherwise
	 */
    protected boolean fillIndexExprs(String fieldName, int matchedFuncExprIndex,
    		AccessPathAnalysisContext analysisCtx) {
    	AqlCompiledIndexDecl primaryIndexDecl = DatasetUtils.getPrimaryIndex(datasetDecl);
    	List<String> primaryIndexFields = primaryIndexDecl.getFieldExprs();    	
        List<AqlCompiledIndexDecl> indexCandidates = DatasetUtils.findSecondaryIndexesByOneOfTheKeys(datasetDecl, fieldName);
        // Check whether the primary index is a candidate. If so, add it to the list.
        if (includePrimaryIndex && primaryIndexFields.contains(fieldName)) {
            if (indexCandidates == null) {
                indexCandidates = new ArrayList<AqlCompiledIndexDecl>(1);
            }
            indexCandidates.add(primaryIndexDecl);
        }
        // No index candidates for fieldName.
        if (indexCandidates == null) {
        	return false;
        }
        // Go through the candidates and fill indexExprs.
        for (AqlCompiledIndexDecl index : indexCandidates) {
        	analysisCtx.addIndexExpr(index, matchedFuncExprIndex);
        }
        return true;
    }

    protected void fillAllIndexExprs(List<LogicalVariable> varList, AccessPathAnalysisContext analysisCtx) {
        for (int varIndex = 0; varIndex < varList.size(); varIndex++) {
            int matchedFuncExprIndex = analysisCtx.findVarInMatchedFuncExprs(varList.get(varIndex));
            // Current var does not match any vars in outComparedVars, 
            // so it's irrelevant for our purpose of optimizing with an index.
            if (matchedFuncExprIndex < 0) {
                continue;
            }            
            String fieldName = null;
            if (assign != null) {
                // Get the fieldName corresponding to the assigned variable at varIndex.
                // If the expr at varIndex is not a fieldAccess we get back null.
                fieldName = getFieldNameOfFieldAccess(assign, recordType, varIndex);
                if (fieldName == null) {
                    continue;
                }
            } else {
				if (!includePrimaryIndex) {
					throw new IllegalStateException(
							"Assign operator not set but only looking for secondary index optimizations.");
				}
                // We don't have an assign, only a datasource scan.
                // The last var. is the record itself, so skip it.
                if (varIndex >= varList.size() - 1) {
                    break;
                }
                // The variable value is one of the partitioning fields.
                fieldName = DatasetUtils.getPartitioningExpressions(datasetDecl).get(varIndex);
            }
            // Set the fieldName in the corresponding matched function expression.
            analysisCtx.setFuncExprFieldName(matchedFuncExprIndex, fieldName);
            // TODO: Don't just ignore open record types.
            if (recordType.isOpen()) {
                continue;
            }
            fillIndexExprs(fieldName, matchedFuncExprIndex, analysisCtx);
        }
    }
    
    /**
     * Returns the field name corresponding to the assigned variable at varIndex.
     * Returns null if the expr at varIndex is not a field access function.
     * 
     * @param assign
     * @param recordType
     * @param varIndex
     * @return
     */
    protected String getFieldNameOfFieldAccess(AssignOperator assign, ARecordType recordType, int varIndex) {
        // Get expression corresponding to var at varIndex.
    	AbstractLogicalExpression assignExpr = (AbstractLogicalExpression) assign.getExpressions()
    			.get(varIndex).getValue();
    	if (assignExpr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
    		return null;
    	}
    	// Analyze the assign op to get the field name
    	// corresponding to the field being assigned at varIndex.
    	AbstractFunctionCallExpression assignFuncExpr = (AbstractFunctionCallExpression) assignExpr;
    	FunctionIdentifier assignFuncIdent = assignFuncExpr.getFunctionIdentifier();
    	if (assignFuncIdent == AsterixBuiltinFunctions.FIELD_ACCESS_BY_NAME) {
    		ILogicalExpression nameArg = assignFuncExpr.getArguments().get(1).getValue();
    		if (nameArg.getExpressionTag() != LogicalExpressionTag.CONSTANT) {
    			return null;
    		}
    		ConstantExpression constExpr = (ConstantExpression) nameArg;
    		return ((AString) ((AsterixConstantValue) constExpr.getValue()).getObject()).getStringValue();
    	} else if (assignFuncIdent == AsterixBuiltinFunctions.FIELD_ACCESS_BY_INDEX) {
    		ILogicalExpression idxArg = assignFuncExpr.getArguments().get(1).getValue();
    		if (idxArg.getExpressionTag() != LogicalExpressionTag.CONSTANT) {
    			return null;
    		}
    		ConstantExpression constExpr = (ConstantExpression) idxArg;
    		int fieldIndex = ((AInt32) ((AsterixConstantValue) constExpr.getValue()).getObject()).getIntegerValue();
    		return recordType.getFieldNames()[fieldIndex];
    	}
    	return null;
    }
}

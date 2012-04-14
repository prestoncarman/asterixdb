package edu.uci.ics.asterix.optimizer.rules.am;

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

/**
 * This rule tries to optimize simple selections with indexes. The use of an
 * index is expressed as an unnest over an index-search function which will be
 * replaced with the appropriate embodiment during codegen.
 * 
 * Matches the following operator patterns:
 * Standard secondary index pattern:
 * There must be at least one assign, but there may be more, e.g., when matching similarity-jaccard-check().
 * (select) <-- (assign)+ <-- (datasource scan)
 * Primary index lookup pattern:
 * Since no assign is necessary to get the primary key fields (they are already stored fields in the BTree tuples).
 * (select) <-- (datasource scan)
 * 
 * Replaces the above patterns with this pattern: (select) <-- (assign) <-- (btree search) <-- (sort) <-- (unnest(index search)) <-- (assign)
 * The sort is optional, and some access methods may choose not to sort.
 * 
 * Note that for some index-based optimizations do not remove the triggering
 * condition from the select, since the index only acts as a filter, and the
 * final verification must still be done via the original function.
 * 
 * The basic outline of this rule is: 
 * 1. Match operator pattern. 
 * 2. Analyze select to see if there are optimizable functions (delegated to IAccessMethods). 
 * 3. Check metadata to see if there are applicable indexes. 
 * 4. Choose an index to apply (for now only a single index will be chosen).
 * 5. Rewrite plan using index (delegated to IAccessMethods).
 * 
 */
public class IntroduceAccessMethodSearchRule implements IAlgebraicRewriteRule {
    
	// Operators representing the patterns to be matched:
    // These ops are set in matchesPattern()
	protected SelectOperator select = null;
	protected List<AssignOperator> assigns = new ArrayList<AssignOperator>();
	protected DataSourceScanOperator dataSourceScan = null;
	// Original operator refs corresponding to the ops above.
	protected Mutable<ILogicalOperator> selectRef = null;
	protected List<Mutable<ILogicalOperator>> assignRefs = new ArrayList<Mutable<ILogicalOperator>>();
	protected Mutable<ILogicalOperator> dataSourceScanRef = null;
	// Condition of the select operator.
	protected AbstractFunctionCallExpression selectCond = null;
	protected boolean includePrimaryIndex = true;
	// Dataaset and type metadata. Set in setDatasetAndTypeMetadata().
	protected ARecordType recordType = null;
	protected AqlCompiledDatasetDecl datasetDecl = null;
	
	protected static Map<FunctionIdentifier, List<IAccessMethod>> accessMethods = new HashMap<FunctionIdentifier, List<IAccessMethod>>();
	static {
	    registerAccessMethod(BTreeAccessMethod.INSTANCE);
	    registerAccessMethod(RTreeAccessMethod.INSTANCE);
	    registerAccessMethod(InvertedIndexAccessMethod.INSTANCE);
	}
	
	public static void registerAccessMethod(IAccessMethod accessMethod) {
	    List<FunctionIdentifier> funcs = accessMethod.getOptimizableFunctions();
	    for (FunctionIdentifier funcIdent : funcs) {
	        List<IAccessMethod> l = accessMethods.get(funcIdent);
	        if (l == null) {
	            l = new ArrayList<IAccessMethod>();
	            accessMethods.put(funcIdent, l);
	        }
	        l.add(accessMethod);
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
        Map<IAccessMethod, AccessMethodAnalysisContext> analyzedAMs = new HashMap<IAccessMethod, AccessMethodAnalysisContext>();
        if (!analyzeSelectCondition(selectCond, analyzedAMs)) {
            return false;
        }

        // Set dataset and type metadata.
        if (!setDatasetAndTypeMetadata((AqlMetadataProvider)context.getMetadataProvider())) {
            return false;
        }
        
        // The assign may be null if there is only a filter on the primary index key.
        // Match variables from lowest assign which comes directly after the dataset scan.
        List<LogicalVariable> varList = (!assigns.isEmpty()) ? assigns.get(assigns.size() - 1).getVariables() : dataSourceScan.getVariables();
        Iterator<Map.Entry<IAccessMethod, AccessMethodAnalysisContext>> amIt = analyzedAMs.entrySet().iterator();
        // Check applicability of indexes by access method type.
        while (amIt.hasNext()) {
            Map.Entry<IAccessMethod, AccessMethodAnalysisContext> entry = amIt.next();
            AccessMethodAnalysisContext amCtx = entry.getValue();
            // For the current access method type, map variables from the assign op to applicable indexes.
            fillAllIndexExprs(varList, amCtx);
            pruneIndexCandidates(entry.getKey(), amCtx);
            // Remove access methods for which there are definitely no applicable indexes.
            if (amCtx.indexExprs.isEmpty()) {
                amIt.remove();
            }
        }
        
        // Choose index to be applied.
        Pair<IAccessMethod, AqlCompiledIndexDecl> chosenIndex = chooseIndex(analyzedAMs);
        if (chosenIndex == null) {
            context.addToDontApplySet(this, select);
            return false;
        }
        
        // Apply plan transformation using chosen index.
        AccessMethodAnalysisContext analysisCtx = analyzedAMs.get(chosenIndex.first);
        Mutable<ILogicalOperator> assignRef = (assignRefs.isEmpty()) ? null : assignRefs.get(0);
        boolean res = chosenIndex.first.applyPlanTransformation(selectRef, assignRef, dataSourceScanRef,
                datasetDecl, recordType, chosenIndex.second, analysisCtx, context);
        if (res) {
            OperatorPropertiesUtil.typeOpRec(opRef, context);
        }
        context.addToDontApplySet(this, select);
        return res;
    }
    
    /**
     * Simply picks the first index that it finds.
     * TODO: Improve this decision process by making it more systematic.
     * 
     * @param datasetDecl
     * @param indexExprs
     * @return
     */
    protected Pair<IAccessMethod, AqlCompiledIndexDecl> chooseIndex(
            Map<IAccessMethod, AccessMethodAnalysisContext> analyzedAMs) {
        Iterator<Map.Entry<IAccessMethod, AccessMethodAnalysisContext>> amIt = analyzedAMs.entrySet().iterator();
        while (amIt.hasNext()) {
            Map.Entry<IAccessMethod, AccessMethodAnalysisContext> amEntry = amIt.next();
            AccessMethodAnalysisContext analysisCtx = amEntry.getValue();
            Iterator<Map.Entry<AqlCompiledIndexDecl, List<Integer>>> indexIt = analysisCtx.indexExprs.entrySet().iterator();
            if (indexIt.hasNext()) {
                Map.Entry<AqlCompiledIndexDecl, List<Integer>> indexEntry = indexIt.next();
                return new Pair<IAccessMethod, AqlCompiledIndexDecl>(amEntry.getKey(), indexEntry.getKey());
            }
        }
        return null;
    }
    
    /**
     * Removes irrelevant access methods candidates, based on whether the
     * expressions in the query match those in the index. For example, some
     * index may require all its expressions to be matched, and some indexes may
     * only require a match on a prefix of fields to be applicable. This methods
     * removes all index candidates indexExprs that are definitely not
     * applicable according to the expressions involved.
     * 
     */
    public void pruneIndexCandidates(IAccessMethod accessMethod, AccessMethodAnalysisContext analysisCtx) {
        Iterator<Map.Entry<AqlCompiledIndexDecl, List<Integer>>> it = analysisCtx.indexExprs.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<AqlCompiledIndexDecl, List<Integer>> entry = it.next();
            AqlCompiledIndexDecl index = entry.getKey();
            Iterator<Integer> exprsIter = entry.getValue().iterator();
            boolean allUsed = true;
            int lastFieldMatched = -1;
            for (int i = 0; i < index.getFieldExprs().size(); i++) {
                String keyField = index.getFieldExprs().get(i);
                boolean foundKeyField = false;
                while (exprsIter.hasNext()) {
                    Integer ix = exprsIter.next();
                    // If expr is not optimizable by concrete index then remove expr and continue.
                    if (!accessMethod.exprIsOptimizable(index, analysisCtx.matchedFuncExprs.get(ix))) {
                        exprsIter.remove();
                        continue;
                    }
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
            // If the access method requires all exprs to be matched but they are not, remove this candidate.
            if (!allUsed && accessMethod.matchAllIndexExprs()) {
                it.remove();
                return;
            }
            // A prefix of the index exprs may have been matched.
            if (lastFieldMatched < 0 && accessMethod.matchPrefixIndexExprs()) {
                it.remove();
                return;
            }
        }
    }
    
    /**
     * Analyzes the given selection condition, filling analyzedAMs with applicable access method types.
     * At this point we are not yet consulting the metadata whether an actual index exists or not.
     * 
     * @param cond
     * @param analyzedAMs
     * @return
     */
    protected boolean analyzeSelectCondition(ILogicalExpression cond, Map<IAccessMethod, AccessMethodAnalysisContext> analyzedAMs) {
        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) cond;
        boolean found = analyzeFunctionExpr(funcExpr, analyzedAMs);
        for (Mutable<ILogicalExpression> arg : funcExpr.getArguments()) {
            ILogicalExpression argExpr = arg.getValue();
            if (argExpr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
                continue;
            }
            AbstractFunctionCallExpression argFuncExpr = (AbstractFunctionCallExpression) argExpr;
            boolean matchFound = analyzeFunctionExpr(argFuncExpr, analyzedAMs); 
            found = found || matchFound;
        }
        return found;
    }
    
    /**
     * Finds applicable access methods for the given function expression based on
     * the function identifier, and an analysis of the function's arguments.
     * Updates the analyzedAMs accordingly.
     * 
     * @param cond
     * @param analyzedAMs
     * @return
     */
    protected boolean analyzeFunctionExpr(AbstractFunctionCallExpression funcExpr, Map<IAccessMethod, AccessMethodAnalysisContext> analyzedAMs) {
        FunctionIdentifier funcIdent = funcExpr.getFunctionIdentifier();        
        if (funcIdent == AlgebricksBuiltinFunctions.AND) {
            return false;
        }
        // Retrieves the list of access methods that are relevant based on the funcIdent.
        List<IAccessMethod> relevantAMs = accessMethods.get(funcIdent);
        if (relevantAMs == null) {
            return false;
        }
        boolean atLeastOneMatchFound = false;
        // Placeholder for a new analysis context in case we need one.
        AccessMethodAnalysisContext newAnalysisCtx = new AccessMethodAnalysisContext();
        for(IAccessMethod accessMethod : relevantAMs) {
            AccessMethodAnalysisContext analysisCtx = analyzedAMs.get(accessMethod);
            // Use the current placeholder.
            if (analysisCtx == null) {
                analysisCtx = newAnalysisCtx;
            }
            // Analyzes the funcExpr's arguments to see if the accessMethod is truly applicable.
            boolean matchFound = accessMethod.analyzeFuncExprArgs(funcExpr, assigns, analysisCtx);
            if (matchFound) {
                // If we've used the current new context placeholder, replace it with a new one.
                if (analysisCtx == newAnalysisCtx) {
                    analyzedAMs.put(accessMethod, analysisCtx);
                    newAnalysisCtx = new AccessMethodAnalysisContext();
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
        // Examine the select's children to match the expected patterns.
        Mutable<ILogicalOperator> opRef2 = op1.getInputs().get(0);
        AbstractLogicalOperator op2 = (AbstractLogicalOperator) opRef2.getValue();   
        // First check primary-index pattern.
        if (op2.getOperatorTag() != LogicalOperatorTag.ASSIGN) {
            // Pattern may still match if we are looking for primary index matches as well.
            if (includePrimaryIndex && op2.getOperatorTag() == LogicalOperatorTag.DATASOURCESCAN) {
                dataSourceScanRef = opRef2;
                dataSourceScan = (DataSourceScanOperator) op2;
                return true;
            }
            return false;
        }
        // Match (assign)+.
        do {
            assignRefs.add(opRef2);
            assigns.add((AssignOperator) op2);
            opRef2 = op2.getInputs().get(0);
            op2 = (AbstractLogicalOperator) opRef2.getValue();   
        } while (op2.getOperatorTag() == LogicalOperatorTag.ASSIGN);
        // Set to last valid assigns.
        opRef2 = assignRefs.get(assignRefs.size() - 1);
        op2 = assigns.get(assigns.size() - 1);
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
    		AccessMethodAnalysisContext analysisCtx) {
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

    protected void fillAllIndexExprs(List<LogicalVariable> varList, AccessMethodAnalysisContext analysisCtx) {
    	for (int optFuncExprIndex = 0; optFuncExprIndex < analysisCtx.matchedFuncExprs.size(); optFuncExprIndex++) {
    		for (int varIndex = 0; varIndex < varList.size(); varIndex++) {
    			LogicalVariable var = varList.get(varIndex);
    			// Vars do not match, so just continue.
    			if (var != analysisCtx.matchedFuncExprs.get(optFuncExprIndex).getLogicalVar()) {
    				continue;
    			}
    			// At this point we have matched the optimizable func expr at optFuncExprIndex to an assigned variable.
    			String fieldName = null;
                if (!assigns.isEmpty()) {
                    // Get the fieldName corresponding to the assigned variable at varIndex
                    // from the assign operator right above the datasource scan.
                    // If the expr at varIndex is not a fieldAccess we get back null.
                    fieldName = getFieldNameOfFieldAccess(assigns.get(assigns.size() - 1), recordType, varIndex);
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
                analysisCtx.setFuncExprFieldName(optFuncExprIndex, fieldName);
                // TODO: Don't just ignore open record types.
                if (recordType.isOpen()) {
                    continue;
                }
                fillIndexExprs(fieldName, optFuncExprIndex, analysisCtx);
    		}
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

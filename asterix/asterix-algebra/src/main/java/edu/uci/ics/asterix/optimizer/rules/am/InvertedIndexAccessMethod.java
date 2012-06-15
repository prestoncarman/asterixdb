package edu.uci.ics.asterix.optimizer.rules.am;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import edu.uci.ics.asterix.algebra.base.LogicalOperatorDeepCopyVisitor;
import edu.uci.ics.asterix.aql.util.FunctionUtils;
import edu.uci.ics.asterix.common.config.DatasetConfig.IndexType;
import edu.uci.ics.asterix.dataflow.data.common.ListEditDistanceSearchModifierFactory;
import edu.uci.ics.asterix.formats.nontagged.AqlBinaryComparatorFactoryProvider;
import edu.uci.ics.asterix.formats.nontagged.AqlBinaryTokenizerFactoryProvider;
import edu.uci.ics.asterix.formats.nontagged.AqlTypeTraitProvider;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledDatasetDecl;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledIndexDecl;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledIndexDecl.IndexKind;
import edu.uci.ics.asterix.om.base.AFloat;
import edu.uci.ics.asterix.om.base.AInt32;
import edu.uci.ics.asterix.om.base.ANull;
import edu.uci.ics.asterix.om.base.AString;
import edu.uci.ics.asterix.om.base.IACollection;
import edu.uci.ics.asterix.om.base.IAObject;
import edu.uci.ics.asterix.om.constants.AsterixConstantValue;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.types.AOrderedListType;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.AUnorderedListType;
import edu.uci.ics.asterix.om.types.AbstractCollectionType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.Triple;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.Counter;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IAlgebricksConstantValue;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.InnerJoinOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.ReplicateOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.UnionAllOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedIndexSearchModifierFactory;
import edu.uci.ics.hyracks.storage.am.invertedindex.searchmodifiers.ConjunctiveSearchModifierFactory;
import edu.uci.ics.hyracks.storage.am.invertedindex.searchmodifiers.EditDistanceSearchModifierFactory;
import edu.uci.ics.hyracks.storage.am.invertedindex.searchmodifiers.JaccardSearchModifierFactory;
import edu.uci.ics.hyracks.storage.am.invertedindex.tokenizers.IBinaryTokenizerFactory;

public class InvertedIndexAccessMethod implements IAccessMethod {

    // Enum describing the search modifier type. Used for passing info to jobgen.
    public static enum SearchModifierType {
        CONJUNCTIVE,
        JACCARD,
        EDIT_DISTANCE,
        INVALID
    }
    
    private static List<FunctionIdentifier> funcIdents = new ArrayList<FunctionIdentifier>();
    static {
        funcIdents.add(AsterixBuiltinFunctions.CONTAINS);        
        // For matching similarity-check functions. For example similarity-jaccard-check returns a list of two items,
        // and the where condition will get the first list-item and check whether it evaluates to true. 
        funcIdents.add(AsterixBuiltinFunctions.GET_ITEM);
    }
    
    // These function identifiers are matched in this AM's analyzeFuncExprArgs(), 
    // and are not visible to the outside driver (IntroduceAccessMethodSearchRule).
    private static HashSet<FunctionIdentifier> secondLevelFuncIdents = new HashSet<FunctionIdentifier>();
    static {
        secondLevelFuncIdents.add(AsterixBuiltinFunctions.SIMILARITY_JACCARD_CHECK);
        secondLevelFuncIdents.add(AsterixBuiltinFunctions.EDIT_DISTANCE_CHECK);
    }
    
    public static InvertedIndexAccessMethod INSTANCE = new InvertedIndexAccessMethod();
    
    @Override
    public List<FunctionIdentifier> getOptimizableFunctions() {
       return funcIdents;
    }

    @Override
    public boolean analyzeFuncExprArgs(AbstractFunctionCallExpression funcExpr, List<AssignOperator> assigns, AccessMethodAnalysisContext analysisCtx) {
        if (funcExpr.getFunctionIdentifier() == AsterixBuiltinFunctions.CONTAINS) {
            return AccessMethodUtils.analyzeFuncExprArgsForOneConstAndVar(funcExpr, analysisCtx);
        }
        return analyzeGetItemFuncExpr(funcExpr, assigns, analysisCtx);
    }
    
    public boolean analyzeGetItemFuncExpr(AbstractFunctionCallExpression funcExpr, List<AssignOperator> assigns, AccessMethodAnalysisContext analysisCtx) {
        if (funcExpr.getFunctionIdentifier() != AsterixBuiltinFunctions.GET_ITEM) {
            return false;
        }
        ILogicalExpression arg1 = funcExpr.getArguments().get(0).getValue();
        ILogicalExpression arg2 = funcExpr.getArguments().get(1).getValue();
        // The second arg is the item index to be accessed. It must be a constant.
        if (arg2.getExpressionTag() != LogicalExpressionTag.CONSTANT) {
            return false;
        }
        // The first arg must be a variable or a function expr.
        // If it is a variable we must track its origin in the assigns to get the original function expr.
        if (arg1.getExpressionTag() != LogicalExpressionTag.VARIABLE &&
                arg1.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }
        AbstractFunctionCallExpression matchedFuncExpr = null;
        // The get-item arg is function call, directly check if it's optimizable.
        if (arg1.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
            matchedFuncExpr = (AbstractFunctionCallExpression) arg1;
        }
        // The get-item arg is a variable. Search the assigns for its origination function.
        int matchedAssignIndex = -1;
        if (arg1.getExpressionTag() == LogicalExpressionTag.VARIABLE) {             
            VariableReferenceExpression varRefExpr = (VariableReferenceExpression) arg1;
            // Try to find variable ref expr in all assigns.
            for (int i = 0; i < assigns.size(); i++) {
                AssignOperator assign = assigns.get(i);
                List<LogicalVariable> assignVars = assign.getVariables();
                List<Mutable<ILogicalExpression>> assignExprs = assign.getExpressions();
                for (int j = 0; j < assignVars.size(); j++) {
                    LogicalVariable var = assignVars.get(j);
                    if (var != varRefExpr.getVariableReference()) {
                        continue;
                    }
                    // We've matched the variable in the first assign. Now analyze the originating function.
                    ILogicalExpression matchedExpr = assignExprs.get(j).getValue();
                    if (matchedExpr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
                        return false;
                    }
                    matchedAssignIndex = i;
                    matchedFuncExpr = (AbstractFunctionCallExpression) matchedExpr;
                    break;
                }
                // We've already found a match.
                if (matchedFuncExpr != null) {
                    break;
                }
            }
        }
        // Check that the matched function is optimizable by this access method.
        if (!secondLevelFuncIdents.contains(matchedFuncExpr.getFunctionIdentifier())) {
            return false;
        }
        boolean selectMatchFound = analyzeSelectSimilarityCheckFuncExprArgs(matchedFuncExpr, assigns, matchedAssignIndex, analysisCtx);
        boolean joinMatchFound = analyzeJoinSimilarityCheckFuncExprArgs(matchedFuncExpr, assigns, matchedAssignIndex, analysisCtx);
        if (selectMatchFound || joinMatchFound) {
            return true;
        }
        return false;
    }

    private boolean analyzeJoinSimilarityCheckFuncExprArgs(AbstractFunctionCallExpression funcExpr,
            List<AssignOperator> assigns, int matchedAssignIndex, AccessMethodAnalysisContext analysisCtx) {
        // There should be exactly three arguments.
        // The last function argument is assumed to be the similarity threshold.
        IAlgebricksConstantValue constThreshVal = null;
        ILogicalExpression arg3 = funcExpr.getArguments().get(2).getValue();
        if (arg3.getExpressionTag() != LogicalExpressionTag.CONSTANT) {
            return false;
        }
        constThreshVal = ((ConstantExpression) arg3).getValue();
        ILogicalExpression arg1 = funcExpr.getArguments().get(0).getValue();
        ILogicalExpression arg2 = funcExpr.getArguments().get(1).getValue();
        // We expect arg1 and arg2 to be non-constants for a join.
        if (arg1.getExpressionTag() == LogicalExpressionTag.CONSTANT 
                || arg2.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
            return false;
        }
        LogicalVariable fieldVar1 = getNonConstArgFieldVar(arg1, funcExpr, assigns, matchedAssignIndex);
        if (fieldVar1 == null) {
            return false;
        }
        LogicalVariable fieldVar2 = getNonConstArgFieldVar(arg2, funcExpr, assigns, matchedAssignIndex);
        if (fieldVar2 == null) {
            return false;
        }
        analysisCtx.matchedFuncExprs.add(new OptimizableFuncExpr(funcExpr, new LogicalVariable[] { fieldVar1, fieldVar2 }, new IAlgebricksConstantValue[] { constThreshVal }));
        return true;
    }
    
    private boolean analyzeSelectSimilarityCheckFuncExprArgs(AbstractFunctionCallExpression funcExpr,
            List<AssignOperator> assigns, int matchedAssignIndex, AccessMethodAnalysisContext analysisCtx) {
        // There should be exactly three arguments.
        // The last function argument is assumed to be the similarity threshold.
        IAlgebricksConstantValue constThreshVal = null;
        ILogicalExpression arg3 = funcExpr.getArguments().get(2).getValue();
        if (arg3.getExpressionTag() != LogicalExpressionTag.CONSTANT) {
            return false;
        }
        constThreshVal = ((ConstantExpression) arg3).getValue();
        ILogicalExpression arg1 = funcExpr.getArguments().get(0).getValue();
        ILogicalExpression arg2 = funcExpr.getArguments().get(1).getValue();
        // Determine whether one arg is constant, and the other is non-constant.
        ILogicalExpression constArg = null;
        ILogicalExpression nonConstArg = null;
        if (arg1.getExpressionTag() == LogicalExpressionTag.CONSTANT 
                && arg2.getExpressionTag() != LogicalExpressionTag.CONSTANT) { 
            constArg = arg1;
            nonConstArg = arg2;
        } else if(arg2.getExpressionTag() == LogicalExpressionTag.CONSTANT
                && arg1.getExpressionTag() != LogicalExpressionTag.CONSTANT) {
            constArg = arg2;
            nonConstArg = arg1;
        } else {
            return false;
        }
        ConstantExpression constExpr = (ConstantExpression) constArg;
        IAlgebricksConstantValue constFilterVal = constExpr.getValue();
        LogicalVariable fieldVar = getNonConstArgFieldVar(nonConstArg, funcExpr, assigns, matchedAssignIndex);
        if (fieldVar == null) {
            return false;
        }
        analysisCtx.matchedFuncExprs.add(new OptimizableFuncExpr(funcExpr, new LogicalVariable[] { fieldVar }, new IAlgebricksConstantValue[] { constFilterVal, constThreshVal }));
        return true;
    }
    
    private LogicalVariable getNonConstArgFieldVar(ILogicalExpression nonConstArg, AbstractFunctionCallExpression funcExpr,
            List<AssignOperator> assigns, int matchedAssignIndex) {
        LogicalVariable fieldVar = null;
        // Analyze nonConstArg depending on similarity function.
        if (funcExpr.getFunctionIdentifier() == AsterixBuiltinFunctions.SIMILARITY_JACCARD_CHECK) {            
            AbstractFunctionCallExpression nonConstFuncExpr = funcExpr;
            if (nonConstArg.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                nonConstFuncExpr = (AbstractFunctionCallExpression) nonConstArg;
                // TODO: Currently, we're only looking for word and gram tokens (non hashed).
                if (nonConstFuncExpr.getFunctionIdentifier() != AsterixBuiltinFunctions.WORD_TOKENS &&
                        nonConstFuncExpr.getFunctionIdentifier() != AsterixBuiltinFunctions.GRAM_TOKENS) {
                    return null;
                }
                // Find the variable that is being tokenized.
                nonConstArg = nonConstFuncExpr.getArguments().get(0).getValue();
            }
            if (nonConstArg.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
                VariableReferenceExpression varExpr = (VariableReferenceExpression) nonConstArg;
                fieldVar = varExpr.getVariableReference();
                // Find expr corresponding to var in assigns below.
                for (int i = matchedAssignIndex + 1; i < assigns.size(); i++) {
                    AssignOperator assign = assigns.get(i);
                    boolean found = false;
                    for (int j = 0; j < assign.getVariables().size(); j++) {
                        if (fieldVar != assign.getVariables().get(j)) {
                            continue;
                        }
                        ILogicalExpression childExpr = assign.getExpressions().get(j).getValue();
                        if (childExpr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
                            break;
                        }
                        AbstractFunctionCallExpression childFuncExpr = (AbstractFunctionCallExpression) childExpr;
                        // If fieldVar references the result of a tokenization, then we should remember the variable being tokenized.
                        if (childFuncExpr.getFunctionIdentifier() != AsterixBuiltinFunctions.WORD_TOKENS &&
                                childFuncExpr.getFunctionIdentifier() != AsterixBuiltinFunctions.GRAM_TOKENS) {
                            break;
                        }
                        // We expect the tokenizer's argument to be a variable, otherwise we cannot apply an index.
                        ILogicalExpression tokArgExpr = childFuncExpr.getArguments().get(0).getValue();
                        if (tokArgExpr.getExpressionTag() != LogicalExpressionTag.VARIABLE) {
                            break;
                        }
                        // Pass the variable being tokenized to the optimizable func expr.
                        VariableReferenceExpression tokArgVarExpr = (VariableReferenceExpression) tokArgExpr;
                        fieldVar = tokArgVarExpr.getVariableReference();
                        found = true;
                        break;
                    }
                    if (found) {
                        break;
                    }
                }
            }
        }
        if (funcExpr.getFunctionIdentifier() == AsterixBuiltinFunctions.EDIT_DISTANCE_CHECK) {
            if (nonConstArg.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
                fieldVar = ((VariableReferenceExpression) nonConstArg).getVariableReference();                
            }
        }
        return fieldVar;
    }
    
    @Override
    public boolean matchAllIndexExprs() {
        return true;
    }

    @Override
    public boolean matchPrefixIndexExprs() {
        return false;
    }

    private UnnestMapOperator createSecondaryToPrimaryPlan(Mutable<ILogicalOperator> selectOrJoinRef, OptimizableOperatorSubTree indexSubTree, OptimizableOperatorSubTree probeSubTree, 
            AqlCompiledIndexDecl chosenIndex, IOptimizableFuncExpr optFuncExpr, boolean retainInput, boolean requiresBroadcast, IOptimizationContext context) throws AlgebricksException {
        AqlCompiledDatasetDecl datasetDecl = indexSubTree.datasetDecl;
        ARecordType recordType = indexSubTree.recordType;
        DataSourceScanOperator dataSourceScan = indexSubTree.dataSourceScan;
        
        InvertedIndexJobGenParams jobGenParams = new InvertedIndexJobGenParams(chosenIndex.getIndexName(), chosenIndex.getKind(), datasetDecl.getName(), retainInput, requiresBroadcast);
        // Add function-specific args such as search modifier, and possibly a similarity threshold.
        addFunctionSpecificArgs(optFuncExpr, jobGenParams);
        // Add the type of search key from the optFuncExpr.
        addSearchKeyType(optFuncExpr, indexSubTree, context, jobGenParams);
        
        // Operator that feeds the secondary-index search.
        AbstractLogicalOperator inputOp = null;
        // Here we generate vars and funcs for assigning the secondary-index keys to be fed into the secondary-index search.
        // List of variables for the assign.
        ArrayList<LogicalVariable> keyVarList = new ArrayList<LogicalVariable>();
        // probeSubTree is null if we are dealing with a selection query, and non-null for join queries.
        if (probeSubTree == null) {
            // List of expressions for the assign.
            ArrayList<Mutable<ILogicalExpression>> keyExprList = new ArrayList<Mutable<ILogicalExpression>>();
            // Add key vars and exprs to argument list.
            addKeyVarsAndExprs(optFuncExpr, keyVarList, keyExprList, context);
            // Assign operator that sets the secondary-index search-key fields.
            inputOp = new AssignOperator(keyVarList, keyExprList);            
            // Input to this assign is the EmptyTupleSource (which the dataSourceScan also must have had as input).
            inputOp.getInputs().add(dataSourceScan.getInputs().get(0));
            inputOp.setExecutionMode(dataSourceScan.getExecutionMode());
        } else {
            // We are optimizing a join. Add the input variable to the secondaryIndexFuncArgs.
            LogicalVariable inputSearchVariable = null;
            if (optFuncExpr.getOperatorSubTree(0) == indexSubTree) {
                // If the index is on a dataset in subtree 0, then subtree 1 will feed.
                inputSearchVariable = optFuncExpr.getLogicalVar(1);
            } else {
                // If the index is on a dataset in subtree 1, then subtree 0 will feed.
                inputSearchVariable = optFuncExpr.getLogicalVar(0);
            }
            keyVarList.add(inputSearchVariable);
            inputOp = (AbstractLogicalOperator) probeSubTree.root;
        }
        jobGenParams.setKeyVarList(keyVarList);
        UnnestMapOperator secondaryIndexUnnestOp = AccessMethodUtils.createSecondaryIndexUnnestMap(datasetDecl,
                recordType, chosenIndex, inputOp, jobGenParams, context, true, retainInput);
        // Generate the rest of the upstream plan which feeds the search results into the primary index.
        UnnestMapOperator primaryIndexUnnestOp = AccessMethodUtils.createPrimaryIndexUnnestMap(dataSourceScan,
                datasetDecl, recordType, secondaryIndexUnnestOp, context, true, retainInput, false);
        return primaryIndexUnnestOp;
    }
    
    @Override
    public boolean applySelectPlanTransformation(Mutable<ILogicalOperator> selectRef, OptimizableOperatorSubTree subTree,
            AqlCompiledIndexDecl chosenIndex, AccessMethodAnalysisContext analysisCtx, IOptimizationContext context)
            throws AlgebricksException {
        IOptimizableFuncExpr optFuncExpr = chooseOptFuncExpr(chosenIndex, analysisCtx);
        UnnestMapOperator primaryIndexUnnestMap = createSecondaryToPrimaryPlan(selectRef, subTree, null, chosenIndex, optFuncExpr, false, false, context);
        // Replace the datasource scan with the new plan rooted at primaryIndexUnnestMap.
        subTree.dataSourceScanRef.setValue(primaryIndexUnnestMap);
        return true;
    }
    
    @Override
    public boolean applyJoinPlanTransformation(Mutable<ILogicalOperator> joinRef,
            OptimizableOperatorSubTree leftSubTree, OptimizableOperatorSubTree rightSubTree,
            AqlCompiledIndexDecl chosenIndex, AccessMethodAnalysisContext analysisCtx, IOptimizationContext context)
            throws AlgebricksException {
        // Figure out if the index is applicable on the left or right side (if both, we arbitrarily prefer the left side).
        AqlCompiledDatasetDecl dataset = analysisCtx.indexDatasetMap.get(chosenIndex);
        // Determine probe and index subtrees based on chosen index.
        OptimizableOperatorSubTree indexSubTree = null;
        OptimizableOperatorSubTree probeSubTree = null;
        if (dataset.getName().equals(leftSubTree.datasetDecl.getName())) {
            indexSubTree = leftSubTree;
            probeSubTree = rightSubTree;
        } else if (dataset.getName().equals(rightSubTree.datasetDecl.getName())) {
            indexSubTree = rightSubTree;
            probeSubTree = leftSubTree;
        }
        IOptimizableFuncExpr optFuncExpr = chooseOptFuncExpr(chosenIndex, analysisCtx);
        
        // Clone the original join condition because we may have to modify it (and we also need the original).
        InnerJoinOperator join = (InnerJoinOperator) joinRef.getValue();
        ILogicalExpression joinCond = join.getCondition().getValue().cloneExpression();
        
        // Create "panic" (non indexed) nested-loop join path if necessary.
        Mutable<ILogicalOperator> panicJoinRef = null;
        if (optFuncExpr.getFuncExpr().getFunctionIdentifier() == AsterixBuiltinFunctions.EDIT_DISTANCE_CHECK) {
            panicJoinRef = new MutableObject<ILogicalOperator>(joinRef.getValue());
            ILogicalOperator newProbeRoot = createPanicNestedLoopJoinPlan(panicJoinRef, indexSubTree, probeSubTree, optFuncExpr, context);
            probeSubTree.rootRef.setValue(newProbeRoot);
            probeSubTree.root = newProbeRoot;
        }
        // Create regular indexed-nested loop join path.
        UnnestMapOperator primaryIndexUnnestMap = createSecondaryToPrimaryPlan(joinRef, indexSubTree, probeSubTree, chosenIndex, optFuncExpr, true, true, context);
        indexSubTree.dataSourceScanRef.setValue(primaryIndexUnnestMap);
        
        // Change join into a select with the same condition.
        SelectOperator topSelect = new SelectOperator(new MutableObject<ILogicalExpression>(joinCond));
        topSelect.getInputs().add(indexSubTree.rootRef);
        joinRef.setValue(topSelect);
        context.computeAndSetTypeEnvironmentForOperator(topSelect);
        
        // Hook up the indexed-nested loop join path with the "panic" (non indexed) nested-loop join path by putting a union all on top.
        if (panicJoinRef != null) {
        	List<LogicalVariable> secondaryIndexProducedVars = new ArrayList<LogicalVariable>();
        	AbstractLogicalOperator secondaryIndexOp = (AbstractLogicalOperator) indexSubTree.dataSourceScanRef.getValue().getInputs().get(0).getValue();
        	// The secondary index unnest may be directly under the primary index unnest, but there may also be an order op in between.
        	if (secondaryIndexOp.getOperatorTag() != LogicalOperatorTag.UNNEST_MAP) {
        		secondaryIndexOp = (AbstractLogicalOperator) secondaryIndexOp.getInputs().get(0).getValue();
        	}
        	VariableUtilities.getProducedVariables(secondaryIndexOp, secondaryIndexProducedVars);
        	
        	List<LogicalVariable> huhu = new ArrayList<LogicalVariable>();
        	VariableUtilities.getProducedVariables(indexSubTree.root, huhu);
        	
        	List<LogicalVariable> v1 = new ArrayList<LogicalVariable>();
        	VariableUtilities.getLiveVariables(joinRef.getValue(), v1);
        	// Remove the output vars of the secondary index search.
        	v1.removeAll(secondaryIndexProducedVars);
        	
        	List<LogicalVariable> v2 = new ArrayList<LogicalVariable>();
        	VariableUtilities.getLiveVariables(panicJoinRef.getValue(), v2);
        	
        	// HARDCODED MAPPING FOR NOW
			List<Triple<LogicalVariable, LogicalVariable, LogicalVariable>> varMap = new ArrayList<Triple<LogicalVariable, LogicalVariable, LogicalVariable>>();
        	varMap.add(new Triple<LogicalVariable, LogicalVariable, LogicalVariable>(v1.get(0), v2.get(0), v1.get(0)));
        	varMap.add(new Triple<LogicalVariable, LogicalVariable, LogicalVariable>(v1.get(1), v2.get(1), v1.get(1)));
        	varMap.add(new Triple<LogicalVariable, LogicalVariable, LogicalVariable>(v1.get(2), v2.get(2), v1.get(2)));
        	varMap.add(new Triple<LogicalVariable, LogicalVariable, LogicalVariable>(v1.get(4), v2.get(3), v1.get(4)));
        	varMap.add(new Triple<LogicalVariable, LogicalVariable, LogicalVariable>(v1.get(5), v2.get(4), v1.get(5)));
			
        	UnionAllOperator unionAllOp = new UnionAllOperator(varMap);
        	unionAllOp.getInputs().add(new MutableObject<ILogicalOperator>(joinRef.getValue()));
        	unionAllOp.getInputs().add(panicJoinRef);
        	context.computeAndSetTypeEnvironmentForOperator(unionAllOp);
        	
        	List<LogicalVariable> usedVars = new ArrayList<LogicalVariable>();
        	VariableUtilities.getUsedVariables(unionAllOp, usedVars);
        	
        	joinRef.setValue(unionAllOp);
        }
        
        return true;
    }
    
    private IOptimizableFuncExpr chooseOptFuncExpr(AqlCompiledIndexDecl chosenIndex, AccessMethodAnalysisContext analysisCtx) {
        // TODO: We can probably do something smarter here based on selectivity.
        // Pick the first expr optimizable by this index.
        List<Integer> indexExprs = analysisCtx.getIndexExprs(chosenIndex);
        int firstExprIndex = indexExprs.get(0);
        return analysisCtx.matchedFuncExprs.get(firstExprIndex);
    }

    private ILogicalOperator createPanicNestedLoopJoinPlan(Mutable<ILogicalOperator> joinRef, OptimizableOperatorSubTree indexSubTree, OptimizableOperatorSubTree probeSubTree, IOptimizableFuncExpr optFuncExpr, IOptimizationContext context) throws AlgebricksException {
        // TODO: This code can be factored out, since we are using it multiple times.
        // We are optimizing a join. Add the input variable to the secondaryIndexFuncArgs.
        LogicalVariable inputSearchVariable = null;
        LogicalVariable joinCondReplaceVar = null;
        if (optFuncExpr.getOperatorSubTree(0) == indexSubTree) {
            // If the index is on a dataset in subtree 0, then subtree 1 will feed.
            inputSearchVariable = optFuncExpr.getLogicalVar(1);
            joinCondReplaceVar = optFuncExpr.getLogicalVar(0);
        } else {
            // If the index is on a dataset in subtree 1, then subtree 0 will feed.
            inputSearchVariable = optFuncExpr.getLogicalVar(0);
            joinCondReplaceVar = optFuncExpr.getLogicalVar(1);
        }
        
        // TODO: Temporarily we are replicating the probe stream, and adding selections to partitioning the stream.
        // What we really want is a partitioning split operator.
        ILogicalOperator replicateOp = new ReplicateOperator(2);
        replicateOp.getInputs().add(new MutableObject<ILogicalOperator>(probeSubTree.root));
        context.computeAndSetTypeEnvironmentForOperator(replicateOp);
        
        // Select operator for removing tuples that are not filterable.
        List<Mutable<ILogicalExpression>> isFilterableArgs = new ArrayList<Mutable<ILogicalExpression>>();
        isFilterableArgs.add(new MutableObject<ILogicalExpression>(new VariableReferenceExpression(inputSearchVariable)));
        ILogicalExpression isFilterableExpr = new ScalarFunctionCallExpression(FunctionUtils.getFunctionInfo(AsterixBuiltinFunctions.EDIT_DISTANCE_STRING_IS_FILTERABLE), isFilterableArgs);        
        SelectOperator isFilterableSelectOp = new SelectOperator(new MutableObject<ILogicalExpression>(isFilterableExpr));
        isFilterableSelectOp.getInputs().add(new MutableObject<ILogicalOperator>(replicateOp));
        context.computeAndSetTypeEnvironmentForOperator(isFilterableSelectOp);
        
        // Select operator for removing tuples that are filterable.
        List<Mutable<ILogicalExpression>> isNotFilterableArgs = new ArrayList<Mutable<ILogicalExpression>>();
        isNotFilterableArgs.add(new MutableObject<ILogicalExpression>(isFilterableExpr));
        ILogicalExpression isNotFilterableExpr = new ScalarFunctionCallExpression(FunctionUtils.getFunctionInfo(AsterixBuiltinFunctions.NOT), isNotFilterableArgs);
        SelectOperator isNotFilterableSelectOp = new SelectOperator(new MutableObject<ILogicalExpression>(isNotFilterableExpr));
        isNotFilterableSelectOp.getInputs().add(new MutableObject<ILogicalOperator>(replicateOp));
        context.computeAndSetTypeEnvironmentForOperator(isNotFilterableSelectOp);
        
        List<LogicalVariable> originalLiveVars = new ArrayList<LogicalVariable>();
        VariableUtilities.getLiveVariables(indexSubTree.root, originalLiveVars);
        int joinCondOrigVarPos = originalLiveVars.indexOf(joinCondReplaceVar);
        
        // Copy the scan subtree in indexSubTree.
        Counter counter = new Counter(context.getVarCounter());
        LogicalOperatorDeepCopyVisitor deepCopyVisitor = new LogicalOperatorDeepCopyVisitor(counter);
        ILogicalOperator scanSubTree = deepCopyVisitor.deepCopy(indexSubTree.root, null);
        context.setVarCounter(counter.get());        
        
        List<LogicalVariable> copyLiveVars = new ArrayList<LogicalVariable>();
        VariableUtilities.getLiveVariables(scanSubTree, copyLiveVars);
        LogicalVariable joinCondNewVar = copyLiveVars.get(joinCondOrigVarPos);
        
		// Replace the inputs of the given join op, and replace variables in its
		// condition since we deep-copied one of the scanner subtrees which
		// changed variables. 
        InnerJoinOperator joinOp = (InnerJoinOperator) joinRef.getValue();
        joinOp.getCondition().getValue().substituteVar(joinCondReplaceVar, joinCondNewVar);
        joinOp.getInputs().clear();
        joinOp.getInputs().add(new MutableObject<ILogicalOperator>(isNotFilterableSelectOp));
        joinOp.getInputs().add(new MutableObject<ILogicalOperator>(scanSubTree));
        context.computeAndSetTypeEnvironmentForOperator(joinOp);
        
        // Return the new root of the probeSubTree.
        return isFilterableSelectOp;
    }
    
    private void addSearchKeyType(IOptimizableFuncExpr optFuncExpr, OptimizableOperatorSubTree indexSubTree, IOptimizationContext context, InvertedIndexJobGenParams jobGenParams) throws AlgebricksException {
        // If we have two variables in the optFunxExpr, then we are optimizing a join.
        IAType type = null;
        ATypeTag typeTag = null;
        if (optFuncExpr.getNumLogicalVars() == 2) {            
            // Find the type of the variable that is going to feed into the index search.
            if (optFuncExpr.getOperatorSubTree(0) == indexSubTree) {
                // If the index is on a dataset in subtree 0, then subtree 1 will feed.
                type = (IAType) context.getOutputTypeEnvironment(optFuncExpr.getOperatorSubTree(1).root).getVarType(optFuncExpr.getLogicalVar(1));
            } else {
                // If the index is on a dataset in subtree 1, then subtree 0 will feed.
                type = (IAType) context.getOutputTypeEnvironment(optFuncExpr.getOperatorSubTree(0).root).getVarType(optFuncExpr.getLogicalVar(0));
            }
            typeTag = type.getTypeTag();
        } else { 
            // We are optimizing a selection query. Add the type of the search key constant.
            AsterixConstantValue constVal = (AsterixConstantValue) optFuncExpr.getConstantVal(0);
            IAObject obj = constVal.getObject();
            type = obj.getType();
            typeTag = type.getTypeTag();
            if (typeTag != ATypeTag.ORDEREDLIST && typeTag != ATypeTag.STRING) {
                throw new AlgebricksException("Only ordered lists and string types supported.");
            }
        }
        jobGenParams.setSearchKeyType(typeTag);
    }
    
    private void addFunctionSpecificArgs(IOptimizableFuncExpr optFuncExpr, InvertedIndexJobGenParams jobGenParams) {
        if (optFuncExpr.getFuncExpr().getFunctionIdentifier() == AsterixBuiltinFunctions.CONTAINS) {
            jobGenParams.setSearchModifierType(SearchModifierType.CONJUNCTIVE);
            jobGenParams.setSimilarityThreshold(new AsterixConstantValue(ANull.NULL));
        }
        if (optFuncExpr.getFuncExpr().getFunctionIdentifier() == AsterixBuiltinFunctions.SIMILARITY_JACCARD_CHECK) {
            jobGenParams.setSearchModifierType(SearchModifierType.JACCARD);
            // Add the similarity threshold which, by convention, is the last constant value.
            jobGenParams.setSimilarityThreshold(optFuncExpr.getConstantVal(optFuncExpr.getNumConstantVals() - 1));
        }
        if (optFuncExpr.getFuncExpr().getFunctionIdentifier() == AsterixBuiltinFunctions.EDIT_DISTANCE_CHECK) {
            jobGenParams.setSearchModifierType(SearchModifierType.EDIT_DISTANCE);
            // Add the similarity threshold which, by convention, is the last constant value.
            jobGenParams.setSimilarityThreshold(optFuncExpr.getConstantVal(optFuncExpr.getNumConstantVals() - 1));
        }
    }

    private void addKeyVarsAndExprs(IOptimizableFuncExpr optFuncExpr, ArrayList<LogicalVariable> keyVarList, ArrayList<Mutable<ILogicalExpression>> keyExprList, IOptimizationContext context) throws AlgebricksException {
        // For now we are assuming a single secondary index key.
        // Add a variable and its expr to the lists which will be passed into an assign op.
        LogicalVariable keyVar = context.newVar();
        keyVarList.add(keyVar);
        keyExprList.add(new MutableObject<ILogicalExpression>(new ConstantExpression(optFuncExpr.getConstantVal(0))));        
        return;
    }

    @Override
    public boolean exprIsOptimizable(AqlCompiledIndexDecl index, IOptimizableFuncExpr optFuncExpr) {
        if (optFuncExpr.getFuncExpr().getFunctionIdentifier() == AsterixBuiltinFunctions.EDIT_DISTANCE_CHECK) {
            // Must be for a join query.
            if (optFuncExpr.getNumConstantVals() == 1) {
                return true;
            }
            // Check for panic in selection query.
            // TODO: Panic also depends on prePost which is currently hardcoded to be true.
            AsterixConstantValue listOrStrConstVal = (AsterixConstantValue) optFuncExpr.getConstantVal(0);
            AsterixConstantValue intConstVal = (AsterixConstantValue) optFuncExpr.getConstantVal(1);
            IAObject listOrStrObj = listOrStrConstVal.getObject();
            IAObject intObj = intConstVal.getObject();
            AInt32 edThresh = (AInt32) intObj;
            int mergeThreshold = 0;
            // We can only optimize edit distance on strings using an ngram index.
            if (listOrStrObj.getType().getTypeTag() == ATypeTag.STRING && index.getKind() == IndexKind.NGRAM_INVIX) {
                AString astr = (AString) listOrStrObj;                    
                // Compute merge threshold.
                mergeThreshold = (astr.getStringValue().length() + index.getGramLength() - 1)
                        - edThresh.getIntegerValue() * index.getGramLength();
            }
            // We can only optimize edit distance on lists using a word index.
            if ((listOrStrObj.getType().getTypeTag() == ATypeTag.ORDEREDLIST || listOrStrObj.getType().getTypeTag() == ATypeTag.UNORDEREDLIST)
                    && index.getKind() == IndexKind.WORD_INVIX) {
                IACollection alist = (IACollection) listOrStrObj;        
                // Compute merge threshold.
                mergeThreshold = alist.size() - edThresh.getIntegerValue();
            }
            if (mergeThreshold <= 0) {
                // We cannot use index to optimize expr.
                return false;
            }
            return true;
        }
        // TODO: We need more checking. Also need to check the gram length, prePost, etc.
        if (optFuncExpr.getFuncExpr().getFunctionIdentifier() == AsterixBuiltinFunctions.SIMILARITY_JACCARD_CHECK) {
            // Check the tokenization function of the non-constant func arg to see if it fits the concrete index type.
            ILogicalExpression arg1 = optFuncExpr.getFuncExpr().getArguments().get(0).getValue();
            ILogicalExpression arg2 = optFuncExpr.getFuncExpr().getArguments().get(1).getValue();
            ILogicalExpression nonConstArg = null;
            if (arg1.getExpressionTag() != LogicalExpressionTag.CONSTANT) {
                nonConstArg = arg1;
            } else {
                nonConstArg = arg2;
            }
            if (nonConstArg.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                AbstractFunctionCallExpression nonConstfuncExpr = (AbstractFunctionCallExpression) nonConstArg;
                // We can use this index if the tokenization function matches the index type.
                if (nonConstfuncExpr.getFunctionIdentifier() == AsterixBuiltinFunctions.WORD_TOKENS &&
                        index.getKind() == IndexKind.WORD_INVIX) {
                    return true;
                }
                if (nonConstfuncExpr.getFunctionIdentifier() == AsterixBuiltinFunctions.GRAM_TOKENS &&
                        index.getKind() == IndexKind.NGRAM_INVIX) {
                    return true;
                }
            }
            // The non-constant arg is not a function call. Perhaps a variable?
            // We must have already verified during our analysis of the select condition, that this variable
            // refers to a list, or to a tokenization function.
            if (nonConstArg.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
                return true;
            }
        }
        // We can only optimize contains with ngram indexes.
        if (optFuncExpr.getFuncExpr().getFunctionIdentifier() == AsterixBuiltinFunctions.CONTAINS
                && index.getKind() == IndexKind.NGRAM_INVIX) {
            // Check that the constant search string has at least gramLength characters.
            AsterixConstantValue strConstVal = (AsterixConstantValue) optFuncExpr.getConstantVal(0);
            IAObject strObj = strConstVal.getObject();
            if (strObj.getType().getTypeTag() == ATypeTag.STRING) {
                AString astr = (AString) strObj;                    
                if (astr.getStringValue().length() >= index.getGramLength()) {
                    return true;
                }
            }
        }
        return false;
    }
    
    public static IBinaryComparatorFactory getTokenBinaryComparatorFactory(IAType keyType) throws AlgebricksException {
        IAType type = keyType;
        ATypeTag typeTag = keyType.getTypeTag();        
        // Extract item type from list.
        if (typeTag == ATypeTag.UNORDEREDLIST || typeTag == ATypeTag.ORDEREDLIST) {
            AbstractCollectionType listType = (AbstractCollectionType) keyType;
            if (!listType.isTyped()) {
                throw new AlgebricksException("Cannot build an inverted index on untyped lists.)");
            }
            type = listType.getItemType();
        }
        // Ignore case for string types.
        return AqlBinaryComparatorFactoryProvider.INSTANCE.getBinaryComparatorFactory(
                type, true, true);
    }
    
    public static ITypeTraits getTokenTypeTrait(IAType keyType) throws AlgebricksException {
        IAType type = keyType;
        ATypeTag typeTag = keyType.getTypeTag();
        // Extract item type from list.
        if (typeTag == ATypeTag.UNORDEREDLIST) {
            AUnorderedListType ulistType = (AUnorderedListType) keyType;
            if (!ulistType.isTyped()) {
                throw new AlgebricksException("Cannot build an inverted index on untyped lists.)");
            }
            type = ulistType.getItemType();
        }
        if (typeTag == ATypeTag.ORDEREDLIST) {
            AOrderedListType olistType = (AOrderedListType) keyType;
            if (!olistType.isTyped()) {
                throw new AlgebricksException("Cannot build an inverted index on untyped lists.)");
            }
            type = olistType.getItemType();
        }
        return AqlTypeTraitProvider.INSTANCE.getTypeTrait(type);
    }
    
    public static IBinaryTokenizerFactory getBinaryTokenizerFactory(SearchModifierType searchModifierType, ATypeTag searchKeyType, AqlCompiledIndexDecl index)
            throws AlgebricksException {
        switch (index.getKind()) {
            case WORD_INVIX: {
                return AqlBinaryTokenizerFactoryProvider.INSTANCE.getWordTokenizerFactory(searchKeyType, false);
            }
            case NGRAM_INVIX: {
                // Make sure not to use pre- and postfixing for conjunctive searches.
                boolean prePost = (searchModifierType == SearchModifierType.CONJUNCTIVE) ? false : true;
                return AqlBinaryTokenizerFactoryProvider.INSTANCE.getNGramTokenizerFactory(searchKeyType,
                        index.getGramLength(), prePost, false);
            }
            default: {
                throw new AlgebricksException("Tokenizer not applicable to index kind '" + index.getKind() + "'.");
            }
        }
    }
    
    public static IBinaryTokenizerFactory getBinaryTokenizerFactory(ATypeTag keyType, IndexType indexType, int gramLength)
            throws AlgebricksException {
        switch (indexType) {
            case WORD_INVIX: {
                return AqlBinaryTokenizerFactoryProvider.INSTANCE.getWordTokenizerFactory(keyType, false);
            }
            case NGRAM_INVIX: {
                return AqlBinaryTokenizerFactoryProvider.INSTANCE.getNGramTokenizerFactory(keyType,
                        gramLength, true, false);
            }
            default: {
                throw new AlgebricksException("Tokenizer not applicable to index type '" + indexType + "'.");
            }
        }
    }
    
    public static IInvertedIndexSearchModifierFactory getSearchModifierFactory(SearchModifierType searchModifierType, IAObject simThresh, AqlCompiledIndexDecl index) throws AlgebricksException {
        switch(searchModifierType) {
            case CONJUNCTIVE: {
                return new ConjunctiveSearchModifierFactory();
            }
            case JACCARD: {
                float jaccThresh = ((AFloat) simThresh).getFloatValue();
                return new JaccardSearchModifierFactory(jaccThresh);
            }
            case EDIT_DISTANCE: {
                int edThresh = ((AInt32) simThresh).getIntegerValue();
                switch (index.getKind()) {
                    case NGRAM_INVIX: {
                        // Edit distance on strings, filtered with overlapping grams.
                        return new EditDistanceSearchModifierFactory(index.getGramLength(), edThresh);
                    }
                    case WORD_INVIX: {
                        // Edit distance on two lists. The list-elements are non-overlapping.
                        return new ListEditDistanceSearchModifierFactory(edThresh);
                    }
                    default: {
                        throw new AlgebricksException("Incompatible search modifier '" + searchModifierType + "' for index type '" + index.getKind() + "'");
                    }
                }
            }
            default: {
                throw new AlgebricksException("Unknown search modifier type '" + searchModifierType + "'.");
            }
        }
    }
}

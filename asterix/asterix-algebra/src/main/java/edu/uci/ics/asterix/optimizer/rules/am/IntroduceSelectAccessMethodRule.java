package edu.uci.ics.asterix.optimizer.rules.am;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.asterix.metadata.declared.AqlCompiledIndexDecl;
import edu.uci.ics.asterix.metadata.declared.AqlMetadataProvider;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;
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
public class IntroduceSelectAccessMethodRule extends AbstractIntroduceAccessMethodRule {
    
	// Operators representing the patterns to be matched:
    // These ops are set in matchesPattern()
    protected Mutable<ILogicalOperator> selectRef = null;
    protected SelectOperator select = null;
	protected AbstractFunctionCallExpression selectCond = null;
	protected final OptimizableOperatorSubTree subTree = new OptimizableOperatorSubTree();

	// Register access methods.
	static {
	    registerAccessMethod(BTreeAccessMethod.INSTANCE);
	    registerAccessMethod(RTreeAccessMethod.INSTANCE);
	    registerAccessMethod(InvertedIndexAccessMethod.INSTANCE);
	}
	
    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
    	// Match operator pattern and initialize operator members.
        if (!matchesOperatorPattern(opRef, context)) {
            return false;
        }
        
        // Analyze select condition.
        Map<IAccessMethod, AccessMethodAnalysisContext> analyzedAMs = new HashMap<IAccessMethod, AccessMethodAnalysisContext>();
        if (!analyzeCondition(selectCond, subTree.assigns, analyzedAMs)) {
            return false;
        }

        // Set dataset and type metadata.
        if (!subTree.setDatasetAndTypeMetadata((AqlMetadataProvider) context.getMetadataProvider())) {
            return false;
        }
        
        fillSubTreeIndexExprs(subTree, analyzedAMs);
        pruneIndexCandidates(analyzedAMs);
        
        // Choose index to be applied.
        Pair<IAccessMethod, AqlCompiledIndexDecl> chosenIndex = chooseIndex(analyzedAMs);
        if (chosenIndex == null) {
            context.addToDontApplySet(this, select);
            return false;
        }
        
        // Apply plan transformation using chosen index.
        AccessMethodAnalysisContext analysisCtx = analyzedAMs.get(chosenIndex.first);
        boolean res = chosenIndex.first.applySelectPlanTransformation(selectRef, subTree, 
                chosenIndex.second, analysisCtx, context);
        if (res) {
            OperatorPropertiesUtil.typeOpRec(opRef, context);
        }
        context.addToDontApplySet(this, select);
        return res;
    }
    
    protected boolean matchesOperatorPattern(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
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
        // Check that the select's condition is a function call.
        ILogicalExpression condExpr = select.getCondition().getValue();
        if (condExpr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }
        selectCond = (AbstractFunctionCallExpression) condExpr;
        return subTree.initFromSubTree(op1.getInputs().get(0));
    }
}

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
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.InnerJoinOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.utils.Pair;

public class IntroduceJoinAccessMethodRule extends AbstractIntroduceAccessMethodRule {
    
    protected Mutable<ILogicalOperator> joinRef = null;
    protected InnerJoinOperator join = null;
	protected AbstractFunctionCallExpression joinCond = null;
	protected final OptimizableOperatorSubTree leftSubTree = new OptimizableOperatorSubTree();	
	protected final OptimizableOperatorSubTree rightSubTree = new OptimizableOperatorSubTree();
	
	// Register access methods.
	static {
	    registerAccessMethod(InvertedIndexAccessMethod.INSTANCE);
	}
	
    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
    	// Match operator pattern and initialize optimizable sub trees.
        if (!matchesOperatorPattern(opRef, context)) {
            return false;
        }
        
        // Analyze condition on those optimizable subtrees that have a datasource scan.
        Map<IAccessMethod, AccessMethodAnalysisContext> analyzedAMs = new HashMap<IAccessMethod, AccessMethodAnalysisContext>();
        boolean matchInLeftSubTree = false;
        boolean matchInRightSubTree = false;
        if (leftSubTree.hasDataSourceScan()) {
            matchInLeftSubTree = analyzeCondition(joinCond, leftSubTree.assigns, analyzedAMs);
        }
        if (rightSubTree.hasDataSourceScan()) {
            matchInRightSubTree = analyzeCondition(joinCond, rightSubTree.assigns, analyzedAMs);
        }
        if (!matchInLeftSubTree && !matchInRightSubTree) {
            return false;
        }

        // Set dataset and type metadata.
        AqlMetadataProvider metadataProvider = (AqlMetadataProvider) context.getMetadataProvider();
        // TODO: Check return values of setting metadata.
        boolean checkLeftSubTreeMetadata = false;
        boolean checkRightSubTreeMetadata = false;
        if (matchInLeftSubTree) {
            checkLeftSubTreeMetadata = leftSubTree.setDatasetAndTypeMetadata(metadataProvider);
        }
        if (matchInRightSubTree) {
            checkRightSubTreeMetadata = rightSubTree.setDatasetAndTypeMetadata(metadataProvider);
        }
        if (!checkLeftSubTreeMetadata && !checkRightSubTreeMetadata) {
            return false;
        }
        if (checkLeftSubTreeMetadata) {
            fillSubTreeIndexExprs(leftSubTree, analyzedAMs);
        }
        if (checkRightSubTreeMetadata) {
            fillSubTreeIndexExprs(rightSubTree, analyzedAMs);
        }
        pruneIndexCandidates(analyzedAMs);
        
        // Choose index to be applied.
        Pair<IAccessMethod, AqlCompiledIndexDecl> chosenIndex = chooseIndex(analyzedAMs);
        if (chosenIndex == null) {
            context.addToDontApplySet(this, join);
            return false;
        }
        
        // Apply plan transformation using chosen index.
        AccessMethodAnalysisContext analysisCtx = analyzedAMs.get(chosenIndex.first);
        boolean res = chosenIndex.first.applyJoinPlanTransformation(joinRef, leftSubTree, rightSubTree,
                chosenIndex.second, analysisCtx, context);
        if (res) {
            OperatorPropertiesUtil.typeOpRec(opRef, context);
        }
        context.addToDontApplySet(this, join);
        return res;
    }
    
    protected boolean matchesOperatorPattern(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        // First check that the operator is a join and its condition is a function call.
        AbstractLogicalOperator op1 = (AbstractLogicalOperator) opRef.getValue();
        if (context.checkIfInDontApplySet(this, op1)) {
            return false;
        }
        // Check op is a select.
        if (op1.getOperatorTag() != LogicalOperatorTag.INNERJOIN) {
            return false;
        }
        // Set and analyze select.
        joinRef = opRef;
        join = (InnerJoinOperator) op1;
        // Check that the select's condition is a function call.
        ILogicalExpression condExpr = join.getCondition().getValue();
        if (condExpr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }
        joinCond = (AbstractFunctionCallExpression) condExpr;
        leftSubTree.initFromSubTree(op1.getInputs().get(0));
        rightSubTree.initFromSubTree(op1.getInputs().get(0));
        // One of the subtrees must have a datasource scan.
        if (leftSubTree.hasDataSourceScan() || rightSubTree.hasDataSourceScan()) {
            return true;
        }
        return false;
    }
}

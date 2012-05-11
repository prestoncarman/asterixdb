package edu.uci.ics.asterix.optimizer.rules.am;

import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.asterix.metadata.declared.AqlCompiledIndexDecl;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;

/**
 * Interface an access method should implement to work with the
 * IntroduceAccessMethodSearchRule that optimizes simple scan-select queries
 * with access methods (the IAccessMethod must still be manually registered in the
 * IntroduceAccessMethodSearchRule). The IntroduceAccessMethodSearchRule will
 * match an operator pattern, and take care of choosing an index to be applied.
 * This interface abstracts away the analysis of the select condition to see
 * whether this access method is applicable. We also provide a method for
 * transforming the plan if an access method of this type has been chosen, since
 * this step is access method specific.
 * 
 */
public interface IAccessMethod {
    
    /**
     * @return A list of function identifiers that are optimizable by this
     *         access method.
     */
    public List<FunctionIdentifier> getOptimizableFunctions();
    
    /**
     * Analyzes the arguments of the given funcExpr to see if funcExpr is
     * optimizable (e.g., one arg is a constant and one is a var). We assume
     * that the funcExpr has already been determined to be optimizable by this
     * access method based on its function identifier. If funcExpr has been
     * found to be optimizable, this method adds an OptimizableFunction to
     * analysisCtx.matchedFuncExprs for further analysis.
     * 
     * @param funcExpr
     * @param assigns
     * @param analysisCtx
     * @return true if funcExpr is optimizable by this access method, false otherwise 
     */
    public boolean analyzeFuncExprArgs(AbstractFunctionCallExpression funcExpr, List<AssignOperator> assigns, AccessMethodAnalysisContext analysisCtx);
    
    /**
     * Indicates whether all index expressions must be matched in order for this
     * index to be applicable.
     * 
     * @return boolean
     */
    public boolean matchAllIndexExprs();

    /** 
     * Indicates whether this index is applicable if only a prefix of the index expressions are matched. 
     * @return boolean
     */
    public boolean matchPrefixIndexExprs();
    
    /**
     * Applies the plan transformation to use chosenIndex to optimize a selection query.
     * 
     */
    public boolean applySelectPlanTransformation(Mutable<ILogicalOperator> selectRef, OptimizableOperatorSubTree subTree,            
            AqlCompiledIndexDecl chosenIndex, AccessMethodAnalysisContext analysisCtx, IOptimizationContext context)
            throws AlgebricksException;
    
    /**
     * Applies the plan transformation to use chosenIndex to optimize a join query.
     * 
     */
    public boolean applyJoinPlanTransformation(Mutable<ILogicalOperator> joinRef, OptimizableOperatorSubTree leftSubTree, OptimizableOperatorSubTree rightSubTree,
            AqlCompiledIndexDecl chosenIndex, AccessMethodAnalysisContext analysisCtx, IOptimizationContext context)
            throws AlgebricksException;
    
    /**
     * Analyzes expr to see whether it is optimizable by the given concrete index. 
     * 
     * @param index
     * @param expr
     * @return
     */
    public boolean exprIsOptimizable(AqlCompiledIndexDecl index, IOptimizableFuncExpr optFuncExpr);
}

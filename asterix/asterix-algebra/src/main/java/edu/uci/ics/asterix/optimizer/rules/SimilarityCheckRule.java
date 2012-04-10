package edu.uci.ics.asterix.optimizer.rules;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.optimizer.rules.am.OptimizableBinaryFuncExpr;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IAlgebricksConstantValue;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * Looks for a select operator, containing a condition:
 * 
 * similarity-function GE/GT/LE/LE constant
 * 
 * Rewrites the plan using the equivalent similarity-check function. This
 * requires adding an assign under the select, which assigns a variable which is
 * the result of the similarity-check function. The select condition is replaced
 * by a get-item on that variable (which evaluates to true/false).
 * 
 */
public class SimilarityCheckRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        // Look for select.        
        if (op.getOperatorTag() != LogicalOperatorTag.SELECT) {
            return false;
        }
        SelectOperator select = (SelectOperator) op;
        Mutable<ILogicalExpression> condExpr = select.getCondition();
        boolean found = replaceSimilarityExprs(condExpr, context);
        
        
        
        return found;
    }

    private boolean replaceSimilarityExprs(Mutable<ILogicalExpression> expRef, IOptimizationContext context) {
        ILogicalExpression expr = expRef.getValue();
        if (expr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }
        if (expr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }
        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;
        FunctionIdentifier funcIdent = funcExpr.getFunctionIdentifier();
        // Recursively traverse conjuncts and disjuncts.
        if (funcIdent == AlgebricksBuiltinFunctions.AND || funcIdent == AlgebricksBuiltinFunctions.OR) {
            boolean found = true;
            for (int i = 0; i < funcExpr.getArguments().size(); ++i) {
                found = found && replaceSimilarityExprs(funcExpr.getArguments().get(i), context);
            }
            return found;
        }
        
        // Look for GE/GT/LE/LT. One arg should be a function call, the other a constant.
        if (funcIdent == AlgebricksBuiltinFunctions.GE || funcIdent == AlgebricksBuiltinFunctions.GT ||
                funcIdent == AlgebricksBuiltinFunctions.LE || funcIdent == AlgebricksBuiltinFunctions.LT) {
            IAlgebricksConstantValue constVal = null;
            AbstractFunctionCallExpression simFuncExpr = null;
            ILogicalExpression arg1 = funcExpr.getArguments().get(0).getValue();
            ILogicalExpression arg2 = funcExpr.getArguments().get(1).getValue();
            // One of the args must be a similarity function call, and the other arg must be a
            // float constant.
            if (arg1.getExpressionTag() == LogicalExpressionTag.CONSTANT
                    && arg2.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                ConstantExpression constExpr = (ConstantExpression) arg1;
                constVal = constExpr.getValue();
                simFuncExpr = (AbstractFunctionCallExpression) arg2;
            } else if (arg1.getExpressionTag() == LogicalExpressionTag.VARIABLE
                    && arg2.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
                ConstantExpression constExpr = (ConstantExpression) arg2;
                constVal = constExpr.getValue();
                simFuncExpr = (AbstractFunctionCallExpression) arg1;
            } else {
                return false;
            }
            
            // Look for jaccard function call.
            if (simFuncExpr.getFunctionIdentifier() == AsterixBuiltinFunctions.SIMILARITY_JACCARD) {
                
            }
            
            // Look for edit-distance function call.
            if(simFuncExpr.getFunctionIdentifier() == AsterixBuiltinFunctions.EDIT_DISTANCE) {
                
            }
            
            return true;
        }
        
        return true;
    }
    
    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        return false;
    }
}

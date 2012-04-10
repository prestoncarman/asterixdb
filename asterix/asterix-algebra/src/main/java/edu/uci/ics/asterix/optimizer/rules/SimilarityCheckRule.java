package edu.uci.ics.asterix.optimizer.rules;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import edu.uci.ics.asterix.aql.util.FunctionUtils;
import edu.uci.ics.asterix.om.base.AFloat;
import edu.uci.ics.asterix.om.base.AInt32;
import edu.uci.ics.asterix.om.constants.AsterixConstantValue;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

/**
 * Looks for a select operator, containing a condition:
 * 
 * similarity-function GE/GT/LE/LE constant
 * 
 * Rewrites the plan using the equivalent similarity-check function. This
 * requires adding an assign under the select. It assigns a variable which is
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
        // In the first phase, we gather all expression replacements. 
        // In the second phase, we assign all necessary variables which the new expressions rely on.
        SelectOperator select = (SelectOperator) op;
        Mutable<ILogicalExpression> condExpr = select.getCondition();
        
        // First is new expression that, second is expression to be replaced by a get-item.
        // List of new similarity-check expressions.
        List<Mutable<ILogicalExpression>> similarityCheckExprs = new ArrayList<Mutable<ILogicalExpression>>();
        // List of old similarity function calls that are going to be replaced by get-item calls.
        List<Mutable<ILogicalExpression>> oldSimilarityExprs = new ArrayList<Mutable<ILogicalExpression>>();
        boolean found = assembleReplacementExprs(condExpr, similarityCheckExprs, oldSimilarityExprs, context);
        if (!found) {
            return false;
        }
        
        List<LogicalVariable> similarityCheckVars = new ArrayList<LogicalVariable>();
        for (int i = 0; i < similarityCheckExprs.size(); i++) {
            LogicalVariable var = context.newVar();
            similarityCheckVars.add(var);
            // Get item 0 from var.
            List<Mutable<ILogicalExpression>> getItemArgs = new ArrayList<Mutable<ILogicalExpression>>();            
            getItemArgs.add(new MutableObject<ILogicalExpression>(new VariableReferenceExpression(var)));
            getItemArgs.add(new MutableObject<ILogicalExpression>(new ConstantExpression(new AsterixConstantValue(new AInt32(0)))));
            ILogicalExpression getItemExpr = new ScalarFunctionCallExpression(FunctionUtils.getFunctionInfo(AsterixBuiltinFunctions.GET_ITEM), getItemArgs);
            // Replace the old similarity function call with the new getItemExpr.
            oldSimilarityExprs.get(i).setValue(getItemExpr);
        }
        
        // Create new assign operator to assign the results of the similarity-check functions to variables.     
        AssignOperator assignOp = new AssignOperator(similarityCheckVars, similarityCheckExprs);
        assignOp.getInputs().add(new MutableObject<ILogicalOperator>(op.getInputs().get(0).getValue()));
        op.getInputs().get(0).setValue(assignOp);
        
        return true;
    }

    private boolean assembleReplacementExprs(Mutable<ILogicalExpression> expRef, List<Mutable<ILogicalExpression>> similarityCheckExprs, List<Mutable<ILogicalExpression>> oldSimilarityExprs , IOptimizationContext context) {
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
                found = found && assembleReplacementExprs(funcExpr.getArguments().get(i), similarityCheckExprs, oldSimilarityExprs, context);
            }
            return found;
        }
        
        // Look for GE/GT/LE/LT.
        if (funcIdent != AlgebricksBuiltinFunctions.GE && funcIdent != AlgebricksBuiltinFunctions.GT &&
                funcIdent != AlgebricksBuiltinFunctions.LE && funcIdent != AlgebricksBuiltinFunctions.LT) {
            return false;
        }

        // One arg should be a function call, the other a constant.
        AsterixConstantValue constVal = null;
        AbstractFunctionCallExpression simFuncExpr = null;
        ILogicalExpression arg1 = funcExpr.getArguments().get(0).getValue();
        ILogicalExpression arg2 = funcExpr.getArguments().get(1).getValue();
        // Normalized GE/GT/LE/LT as if constant was on the right hand side.
        FunctionIdentifier normFuncIdent = null;
        // One of the args must be a similarity function call, and the other arg must be a
        // float constant.
        if (arg1.getExpressionTag() == LogicalExpressionTag.CONSTANT
                && arg2.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
            ConstantExpression constExpr = (ConstantExpression) arg1;
            constVal = (AsterixConstantValue) constExpr.getValue();
            simFuncExpr = (AbstractFunctionCallExpression) arg2;
            // Get func ident as if swapping lhs and rhs.            
            normFuncIdent = getLhsAndRhsSwappedFuncIdent(funcIdent);
        } else if (arg1.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL
                && arg2.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
            ConstantExpression constExpr = (ConstantExpression) arg2;
            constVal = (AsterixConstantValue) constExpr.getValue();
            simFuncExpr = (AbstractFunctionCallExpression) arg1;
            // Constant is already on rhs, so nothing to be done for normalizedFuncIdent.
            normFuncIdent = funcIdent;
        } else {
            return false;
        }

        // Remember args from original similarity function to add them to the similarity-check function later.
        ArrayList<Mutable<ILogicalExpression>> similarityArgs = new ArrayList<Mutable<ILogicalExpression>>();
        similarityArgs.addAll(simFuncExpr.getArguments());
        
        ScalarFunctionCallExpression simCheckFuncExpr = null; 
        
        // Look for jaccard function call, and GE or GT.
        if (simFuncExpr.getFunctionIdentifier() == AsterixBuiltinFunctions.SIMILARITY_JACCARD) {
            AFloat aFloat = (AFloat) constVal.getObject();
            AFloat jaccThresh;
            if (normFuncIdent == AlgebricksBuiltinFunctions.GE) {
                jaccThresh = aFloat;
            } else if (normFuncIdent == AlgebricksBuiltinFunctions.GT) {
                float f = aFloat.getFloatValue() + Float.MIN_VALUE;
                if (f > 1.0f) f = 1.0f;
                jaccThresh = new AFloat(f);
            } else {
                return false;
            }
            similarityArgs.add(new MutableObject<ILogicalExpression>(new ConstantExpression(new AsterixConstantValue(jaccThresh))));
            simCheckFuncExpr = new ScalarFunctionCallExpression(
                    FunctionUtils.getFunctionInfo(AsterixBuiltinFunctions.SIMILARITY_JACCARD_CHECK), similarityArgs);
        }

        // Look for edit-distance function call, and LE or LT.
        if(simFuncExpr.getFunctionIdentifier() == AsterixBuiltinFunctions.EDIT_DISTANCE) {
            AInt32 aInt = (AInt32) constVal.getObject();
            AInt32 edThresh;
            if (normFuncIdent == AlgebricksBuiltinFunctions.LE) {
                edThresh = aInt;
            } else if (normFuncIdent == AlgebricksBuiltinFunctions.LT) {
                int ed = aInt.getIntegerValue() - 1;
                if (ed < 0) ed = 0;
                edThresh = new AInt32(ed);
            } else {
                return false;
            }
            similarityArgs.add(new MutableObject<ILogicalExpression>(new ConstantExpression(new AsterixConstantValue(edThresh))));
            simCheckFuncExpr = new ScalarFunctionCallExpression(
                    FunctionUtils.getFunctionInfo(AsterixBuiltinFunctions.EDIT_DISTANCE_CHECK), similarityArgs);
        }
        
        if (simCheckFuncExpr != null) {
            similarityCheckExprs.add(new MutableObject<ILogicalExpression>(simCheckFuncExpr));
            oldSimilarityExprs.add(expRef);
            return true;
        }

        return false;
    }
    
    private FunctionIdentifier getLhsAndRhsSwappedFuncIdent(FunctionIdentifier oldFuncIdent) {
        if (oldFuncIdent == AlgebricksBuiltinFunctions.GE) {
            return AlgebricksBuiltinFunctions.LE;
        }
        if (oldFuncIdent == AlgebricksBuiltinFunctions.GT) {
            return AlgebricksBuiltinFunctions.LT;
        }
        if (oldFuncIdent == AlgebricksBuiltinFunctions.LE) {
            return AlgebricksBuiltinFunctions.GE;
        }
        if (oldFuncIdent == AlgebricksBuiltinFunctions.LT) {
            return AlgebricksBuiltinFunctions.GT;
        }
        throw new IllegalStateException();
    }
    
    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        return false;
    }
}

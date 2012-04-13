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
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
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
 * Rewrites the select condition with the equivalent similarity-check function.
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
        
        return replaceSelectConditionExprs(condExpr, context);
    }

    private boolean replaceSelectConditionExprs(Mutable<ILogicalExpression> expRef, IOptimizationContext context) {
        ILogicalExpression expr = expRef.getValue();
        if (expr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }
        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) expr;
        FunctionIdentifier funcIdent = funcExpr.getFunctionIdentifier();
        // Recursively traverse conjuncts and disjuncts.
        if (funcIdent == AlgebricksBuiltinFunctions.AND || funcIdent == AlgebricksBuiltinFunctions.OR) {
            boolean found = true;
            for (int i = 0; i < funcExpr.getArguments().size(); ++i) {
                found = found && replaceSelectConditionExprs(funcExpr.getArguments().get(i), context);
            }
            return found;
        }
        
        // Look for GE/GT/LE/LT.
        if (funcIdent != AlgebricksBuiltinFunctions.GE && funcIdent != AlgebricksBuiltinFunctions.GT &&
                funcIdent != AlgebricksBuiltinFunctions.LE && funcIdent != AlgebricksBuiltinFunctions.LT) {
            return false;
        }

        // One arg should be a function call or a variable, the other a constant.
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
        
        // Replace the expr in the select condition.
        if (simCheckFuncExpr != null) {
            // Get item 0 from var.
            List<Mutable<ILogicalExpression>> getItemArgs = new ArrayList<Mutable<ILogicalExpression>>();
            // First arg is the similarity-check function call.
            getItemArgs.add(new MutableObject<ILogicalExpression>(simCheckFuncExpr));
            // Second arg is the item index to be accessed.
            getItemArgs.add(new MutableObject<ILogicalExpression>(new ConstantExpression(new AsterixConstantValue(new AInt32(0)))));
            ILogicalExpression getItemExpr = new ScalarFunctionCallExpression(FunctionUtils.getFunctionInfo(AsterixBuiltinFunctions.GET_ITEM), getItemArgs);
            // Replace the old similarity function call with the new getItemExpr.
            expRef.setValue(getItemExpr);
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

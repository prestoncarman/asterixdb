package edu.uci.ics.asterix.optimizer.rules.am;

import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;

public interface IOptimizableFuncExpr {
    public AbstractFunctionCallExpression getFuncExpr();        
}

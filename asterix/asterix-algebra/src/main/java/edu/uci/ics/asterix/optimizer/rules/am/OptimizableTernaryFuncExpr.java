package edu.uci.ics.asterix.optimizer.rules.am;

import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IAlgebricksConstantValue;

public class OptimizableTernaryFuncExpr extends OptimizableBinaryFuncExpr {
    protected final IAlgebricksConstantValue secondConstantVal;

    public OptimizableTernaryFuncExpr(AbstractFunctionCallExpression funcExpr, IAlgebricksConstantValue constantVal,
            IAlgebricksConstantValue secondConstantVal, LogicalVariable logicalVar) {
        super(funcExpr, constantVal, logicalVar);
        this.secondConstantVal = secondConstantVal;
    }
    
    public IAlgebricksConstantValue getSecondConstVal() {
        return secondConstantVal;
    }
}

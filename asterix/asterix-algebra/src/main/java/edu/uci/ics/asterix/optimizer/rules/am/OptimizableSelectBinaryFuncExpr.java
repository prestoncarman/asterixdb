package edu.uci.ics.asterix.optimizer.rules.am;

import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IAlgebricksConstantValue;

public class OptimizableSelectBinaryFuncExpr implements IOptimizableFuncExpr {
    protected final AbstractFunctionCallExpression funcExpr;
    protected final IAlgebricksConstantValue constantVal;
    protected final LogicalVariable logicalVar;
    protected String fieldName;
    
    public OptimizableSelectBinaryFuncExpr(AbstractFunctionCallExpression funcExpr, IAlgebricksConstantValue constantVal, LogicalVariable logicalVar) {
        this.funcExpr = funcExpr;
        this.constantVal = constantVal;
        this.logicalVar = logicalVar;
    }
    
    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }
    
    public AbstractFunctionCallExpression getFuncExpr() {
        return funcExpr;
    }
    
    public IAlgebricksConstantValue getConstVal() {
        return constantVal;
    }
    
    public LogicalVariable getLogicalVar() {
        return logicalVar;
    }
    
    public String getFieldName() {
        return fieldName;
    }
    
    // Returns true if the constant value is on the "left hand side" (assuming a binary function).
    public boolean constantIsOnLhs() {
        return funcExpr.getArguments().get(0) == constantVal;
    }
}

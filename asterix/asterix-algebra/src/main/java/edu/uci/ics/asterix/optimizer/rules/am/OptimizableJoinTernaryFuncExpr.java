package edu.uci.ics.asterix.optimizer.rules.am;

import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IAlgebricksConstantValue;

public class OptimizableJoinTernaryFuncExpr implements IOptimizableFuncExpr {
    protected final AbstractFunctionCallExpression funcExpr;
    protected final LogicalVariable[] logicalVars = new LogicalVariable[2];
    protected final String[] fieldNames = new String[2];
    protected final IAlgebricksConstantValue constantVal;
    
    public OptimizableJoinTernaryFuncExpr(AbstractFunctionCallExpression funcExpr, LogicalVariable firstLogicalVar, LogicalVariable secondLogicalVar, IAlgebricksConstantValue constantVal) {
        this.funcExpr = funcExpr;
        logicalVars[0] = firstLogicalVar;
        logicalVars[1] = secondLogicalVar;
        this.constantVal = constantVal;
    }
    
    public void setFieldName(int index, String fieldName) {
        fieldNames[index] = fieldName;
    }
    
    public String getFieldName(int index) {
        return fieldNames[index];
    }
    
    public LogicalVariable getLogicalVar(int index) {
        return logicalVars[index];
    }
    
    public AbstractFunctionCallExpression getFuncExpr() {
        return funcExpr;
    }
    
    public IAlgebricksConstantValue getConstVal() {
        return constantVal;
    }
}

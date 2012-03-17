package edu.uci.ics.asterix.optimizer.rules;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.asterix.metadata.declared.AqlCompiledIndexDecl;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;

public class AccessPathAnalysisContext {
    
    public List<OptimizableFuncExpr> matchedFuncExprs = new ArrayList<OptimizableFuncExpr>();
    public List<Mutable<ILogicalExpression>> remainingFuncExprs = new ArrayList<Mutable<ILogicalExpression>>();
    // Contains candidate indexes and a list of integers that index into matchedFuncExprs.
    // In effect, we are mapping from candidate indexes to a list of function expressions 
    // that match one of the index's expressions.
    public HashMap<AqlCompiledIndexDecl, List<Integer>> indexExprs = new HashMap<AqlCompiledIndexDecl, List<Integer>>();
    
    public int findVarInMatchedFuncExprs(LogicalVariable var) {
    	int outVarIndex = 0;
    	while (outVarIndex < matchedFuncExprs.size()) {
    		if (var == matchedFuncExprs.get(outVarIndex).getLogicalVar()) {
    			return outVarIndex;
    		}
    		outVarIndex++;
    	}
    	return -1;
    }
    
    public void setFuncExprFieldName(int matchedFuncExprIndex, String fieldName) {
    	matchedFuncExprs.get(matchedFuncExprIndex).setFieldName(fieldName);
    }
    
    public void addIndexExpr(AqlCompiledIndexDecl index, Integer exprIndex) {
    	List<Integer> exprs = indexExprs.get(index);
    	if (exprs == null) {
    	    exprs = new ArrayList<Integer>();
    		indexExprs.put(index, exprs);
    	}
    	exprs.add(exprIndex);
    }
    
    public List<Integer> getIndexExprs(AqlCompiledIndexDecl index) {
        return indexExprs.get(index);
    }
}

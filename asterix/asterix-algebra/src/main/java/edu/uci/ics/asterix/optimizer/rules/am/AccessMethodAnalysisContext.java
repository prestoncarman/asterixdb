package edu.uci.ics.asterix.optimizer.rules.am;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.asterix.metadata.declared.AqlCompiledIndexDecl;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;

public class AccessMethodAnalysisContext {
    
    public List<OptimizableBinaryFuncExpr> matchedFuncExprs = new ArrayList<OptimizableBinaryFuncExpr>();
    public List<Mutable<ILogicalExpression>> remainingFuncExprs = new ArrayList<Mutable<ILogicalExpression>>();
    // Contains candidate indexes and a list of integers that index into matchedFuncExprs.
    // In effect, we are mapping from candidate indexes to a list of function expressions 
    // that match one of the index's expressions.
    public HashMap<AqlCompiledIndexDecl, List<Integer>> indexExprs = new HashMap<AqlCompiledIndexDecl, List<Integer>>();
    
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

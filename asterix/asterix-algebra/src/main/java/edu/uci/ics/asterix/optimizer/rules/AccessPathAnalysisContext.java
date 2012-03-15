package edu.uci.ics.asterix.optimizer.rules;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.asterix.metadata.declared.AqlCompiledIndexDecl;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;

public class AccessPathAnalysisContext {
    // TODO: These belong to the BTree only. Probably remove them later.
    private enum LimitType {
        LOW_INCLUSIVE, LOW_EXCLUSIVE, HIGH_INCLUSIVE, HIGH_EXCLUSIVE, EQUAL
    }
    /*
    // ALEX: Remember this was foundExprList
    // List of function expressions that match this access path.
    // The current assumption is that one side is a variable and one side is a constant.
    public List<ILogicalExpression> matchedFuncExprs = new ArrayList<ILogicalExpression>();
    // Refers to matchedFuncExprs. List of constants.
    public List<IAlgebricksConstantValue> matchedFuncConstants = new ArrayList<IAlgebricksConstantValue>();
    // Refers to matchedFuncExprs. List of variables.
    public List<LogicalVariable> matchedFuncVars = new ArrayList<LogicalVariable>();
    // Originating field names of variables in matchedFuncExprs. 
    // Entries could be null if they don't refer to a field in a dataset. 
    public List<String> matchedFieldNames = new ArrayList<String>();
    */
    List<OptimizableFuncExpr> matchedFuncExprs = new ArrayList<OptimizableFuncExpr>();
    public List<LimitType> outLimits = new ArrayList<LimitType>();
    public List<Mutable<ILogicalExpression>> outRest = new ArrayList<Mutable<ILogicalExpression>>();
    // Contains candidate indexes and a list of integers that index into matchedFuncExprs.
    // In effect, we are mapping from candidate indexes to a list of function expressions 
    // that match one of the index's expressions.
    public HashMap<AqlCompiledIndexDecl, List<Integer>> indexExprs = new HashMap<AqlCompiledIndexDecl, List<Integer>>();
}

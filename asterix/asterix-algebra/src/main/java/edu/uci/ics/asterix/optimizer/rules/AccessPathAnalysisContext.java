package edu.uci.ics.asterix.optimizer.rules;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.asterix.metadata.declared.AqlCompiledIndexDecl;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IAlgebricksConstantValue;
import edu.uci.ics.hyracks.algebricks.core.utils.Pair;

public class AccessPathAnalysisContext {
    // TODO: These belong to the BTree only. Probably remove them later.
    private enum LimitType {
        LOW_INCLUSIVE, LOW_EXCLUSIVE, HIGH_INCLUSIVE, HIGH_EXCLUSIVE, EQUAL
    }
    
    public ArrayList<IAlgebricksConstantValue> outFilters = new ArrayList<IAlgebricksConstantValue>();
    public ArrayList<LogicalVariable> outComparedVars = new ArrayList<LogicalVariable>();
    public ArrayList<LimitType> outLimits = new ArrayList<LimitType>();
    public ArrayList<Mutable<ILogicalExpression>> outRest = new ArrayList<Mutable<ILogicalExpression>>();
    // ALEX: Remember this was foundExprList
    public ArrayList<ILogicalExpression> matchedExprs = new ArrayList<ILogicalExpression>();
    // Contains candidate indexes and corresponding expressions that we could optimize.
    // Maps from index to a list of pairs. Each list-entry is a <fieldName,varPos> pair. 
    // The varPos is an index into outComparedVars, and fieldName is the corresponding fieldName.
    public HashMap<AqlCompiledIndexDecl, List<Pair<String, Integer>>> indexExprs = new HashMap<AqlCompiledIndexDecl, List<Pair<String, Integer>>>();
}

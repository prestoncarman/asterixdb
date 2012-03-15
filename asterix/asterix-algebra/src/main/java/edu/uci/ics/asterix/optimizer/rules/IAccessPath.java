package edu.uci.ics.asterix.optimizer.rules;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.asterix.metadata.declared.AqlCompiledDatasetDecl;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledIndexDecl;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IAlgebricksConstantValue;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.utils.Pair;

public interface IAccessPath {
    public List<FunctionIdentifier> getOptimizableFunctions();
    
    public boolean analyzeFuncExprArgs(AbstractFunctionCallExpression funcExpr, AccessPathAnalysisContext analysisCtx);
 
    /** 
     * Indicates whether all index expressions must be matched in order for this index to be applicable. 
     * @return boolean
     */
    public boolean matchAllIndexExprs();

    /** 
     * Indicates whether this index is applicable if only a prefix of the index expressions are matched. 
     * @return boolean
     */
    public boolean matchPrefixIndexExprs();
    
    public void applyPlanTransformation(Mutable<ILogicalOperator> dataSourceScanRef, DataSourceScanOperator dataSourceScan,
            AssignOperator assign, ArrayList<IAlgebricksConstantValue> filters,
            AqlCompiledDatasetDecl datasetDecl, AqlCompiledIndexDecl chosenIndex, 
            HashMap<AqlCompiledIndexDecl, List<Pair<String, Integer>>> indexExprs, ARecordType recordType, IOptimizationContext context) throws AlgebricksException;
}

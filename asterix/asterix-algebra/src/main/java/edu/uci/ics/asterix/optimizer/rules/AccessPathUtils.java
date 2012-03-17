package edu.uci.ics.asterix.optimizer.rules;

import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.asterix.metadata.declared.AqlCompiledDatasetDecl;
import edu.uci.ics.asterix.metadata.utils.DatasetUtils;
import edu.uci.ics.asterix.om.base.AString;
import edu.uci.ics.asterix.om.constants.AsterixConstantValue;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IAlgebricksConstantValue;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.base.IEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.core.utils.Triple;

public class AccessPathUtils {
	public static List<Object> primaryIndexTypes(AqlCompiledDatasetDecl datasetDecl, IAType itemType) {
        List<Object> types = new ArrayList<Object>();
        List<Triple<IEvaluatorFactory, ScalarFunctionCallExpression, IAType>> partitioningFunctions = DatasetUtils
                .getPartitioningFunctions(datasetDecl);
        for (Triple<IEvaluatorFactory, ScalarFunctionCallExpression, IAType> t : partitioningFunctions) {
            types.add(t.third);
        }
        types.add(itemType);
        return types;
    }
	
	public static ConstantExpression createStringConstant(String str) {
        return new ConstantExpression(new AsterixConstantValue(new AString(str)));
    }
	
    public static boolean analyzeFuncExprArgsForOneConstAndVar(AbstractFunctionCallExpression funcExpr,
            AccessPathAnalysisContext analysisCtx) {
        IAlgebricksConstantValue constFilterVal = null;
        LogicalVariable fieldVar = null;
        ILogicalExpression arg1 = funcExpr.getArguments().get(0).getValue();
        ILogicalExpression arg2 = funcExpr.getArguments().get(1).getValue();
        // One of the args must be a constant, and the other arg must be a
        // variable.
        if (arg1.getExpressionTag() == LogicalExpressionTag.CONSTANT
                && arg2.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
            ConstantExpression constExpr = (ConstantExpression) arg1;
            constFilterVal = constExpr.getValue();
            VariableReferenceExpression varExpr = (VariableReferenceExpression) arg2;
            fieldVar = varExpr.getVariableReference();
        } else if (arg1.getExpressionTag() == LogicalExpressionTag.VARIABLE
                && arg2.getExpressionTag() == LogicalExpressionTag.CONSTANT) {
            ConstantExpression constExpr = (ConstantExpression) arg2;
            constFilterVal = constExpr.getValue();
            VariableReferenceExpression varExpr = (VariableReferenceExpression) arg1;
            fieldVar = varExpr.getVariableReference();
        } else {
            return false;
        }
        analysisCtx.matchedFuncExprs.add(new OptimizableFuncExpr(funcExpr, constFilterVal, fieldVar));
        return true;
    }
}

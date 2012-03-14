package edu.uci.ics.asterix.optimizer.rules;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.asterix.metadata.declared.AqlCompiledDatasetDecl;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledIndexDecl;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledMetadataDeclarations;
import edu.uci.ics.asterix.metadata.utils.DatasetUtils;
import edu.uci.ics.asterix.om.base.AInt32;
import edu.uci.ics.asterix.om.base.AString;
import edu.uci.ics.asterix.om.constants.AsterixConstantValue;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractLogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.SelectOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.base.IEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import edu.uci.ics.hyracks.algebricks.core.utils.Pair;
import edu.uci.ics.hyracks.algebricks.core.utils.Triple;

/**
 * This rule tries to optimize simple selections with indexes.
 * 
 * Matches this operator pattern: (select) <-- (assign) <-- (datasource scan)
 * Replaces it with this pattern: (select) <-- (assign) <-- (btree search) <-- (index search)
 * 
 * For the special case of primary index lookups it may also match the following pattern:
 * (select) <-- (datasource scan)
 * 
 * Note that for some index-based optimizations do not remove the triggering condition from the select, 
 * since the index only acts as a filter, and the final verification must still be done 
 * via the original function.
 * 
 * The basic outline of this rule is:
 * 1. Match operator pattern.
 * 2. Analyze select to see if there are optimizable functions.
 * 3. Check metadata to see if there are applicable indexex.
 * 4. Choose an index to apply.
 * 4. Rewrite plan using index.
 * 
 */
// TODO: Rename this to IntroduceIndexSearchRule because secondary inverted indexes may also apply.
public abstract class IntroduceTreeIndexSearchRule implements IAlgebraicRewriteRule {

	// Operators representing the pattern to be matched:
	// (select) <-- (assign) <-- (datasource scan)
    // OR
	// (select) <-- (datasource scan)
    // These ops are set in matchesPattern()
	protected SelectOperator select = null;
	protected AssignOperator assign = null;
	protected DataSourceScanOperator dataSourceScan = null;
	// Original operator refs corresponding to the ops above.
	protected Mutable<ILogicalOperator> selectRef = null;
	protected Mutable<ILogicalOperator> assignRef = null;
	protected Mutable<ILogicalOperator> dataSourceScanRef = null;
	// Condition of the select operator.
	protected AbstractFunctionCallExpression selectCond = null;
	protected boolean includePrimaryIndex = true;
	
    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        return false;
    }

    protected abstract boolean matchesConcreteRule(FunctionIdentifier funcIdent);
    
    protected boolean matchesPattern(Mutable<ILogicalOperator> opRef, IOptimizationContext context, boolean includePrimaryIndex) {
        this.includePrimaryIndex = includePrimaryIndex;
        // First check that the operator is a select and its condition is a function call.
        AbstractLogicalOperator op1 = (AbstractLogicalOperator) opRef.getValue();
        if (context.checkIfInDontApplySet(this, op1)) {
            return false;
        }
        // Check op is a select.
        if (op1.getOperatorTag() != LogicalOperatorTag.SELECT) {
            return false;
        }
        // Set and analyze select.
        selectRef = opRef;
        select = (SelectOperator) op1;
        ILogicalExpression condExpr = select.getCondition().getValue();
        if (condExpr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }
        // Check that the select's condition is a function call.
        selectCond = (AbstractFunctionCallExpression) condExpr;
        FunctionIdentifier funcIdent = selectCond.getFunctionIdentifier();
        // Make sure the function matches the concrete rule's spec 
        // or an AND condition. We will look at the conjuncts one-by-one in analyzeCondition().
        if (!matchesConcreteRule(funcIdent) && funcIdent != AlgebricksBuiltinFunctions.AND) {
            return false;
        }
        // Examine the select's children to match the expected pattern:
        // (select) <-- (assign) <-- (datasource scan)
        // OR
        // (select) <-- (datasource scan)
        // Match assign.
        Mutable<ILogicalOperator> opRef2 = op1.getInputs().get(0);
        AbstractLogicalOperator op2 = (AbstractLogicalOperator) opRef2.getValue();
        // Match assign.
        if (op2.getOperatorTag() != LogicalOperatorTag.ASSIGN) {
            // Pattern may still match if we are looking for primary index matches as well.
            if (includePrimaryIndex && op2.getOperatorTag() == LogicalOperatorTag.DATASOURCESCAN) {
                dataSourceScanRef = opRef2;
                dataSourceScan = (DataSourceScanOperator) op2;
                return true;
            }
            return false;
        }
        assignRef = opRef2;
        assign = (AssignOperator) op2;
        // Match datasource scan.
        Mutable<ILogicalOperator> opRef3 = op2.getInputs().get(0);
        AbstractLogicalOperator op3 = (AbstractLogicalOperator) opRef3.getValue();
        if (op3.getOperatorTag() != LogicalOperatorTag.DATASOURCESCAN) {
            return false;
        }
        dataSourceScanRef = opRef3;
        dataSourceScan = (DataSourceScanOperator) op3;
        return true;
    }
    
	/**
	 * Picks the first index for which all the expressions are mentioned.
	 */
	protected AqlCompiledIndexDecl chooseIndex(
			AqlCompiledDatasetDecl datasetDecl,
			HashMap<AqlCompiledIndexDecl, List<Pair<String, Integer>>> indexExprs) {
		for (AqlCompiledIndexDecl index : indexExprs.keySet()) {
			List<Pair<String, Integer>> psiList = indexExprs.get(index);
			boolean allUsed = true;
			for (String keyField : index.getFieldExprs()) {
				boolean foundKeyField = false;
				for (Pair<String, Integer> psi : psiList) {
					if (psi.first.equals(keyField)) {
						foundKeyField = true;
						break;
					}
				}
				if (!foundKeyField) {
					allUsed = false;
					break;
				}
			}
			if (allUsed) {
				return index;
			}
		}
		return null;
	}

    protected static ConstantExpression createStringConstant(String str) {
        return new ConstantExpression(new AsterixConstantValue(new AString(str)));
    }

	/**
	 * 
	 * @param datasetDecl
	 * @param foundIndexExprs
	 * @param comparedVars
	 * @param var
	 * @param fieldName
	 * @return returns true if a candidate index was added to foundIndexExprs,
	 *         false otherwise
	 */
    protected boolean fillIndexExprs(AqlCompiledDatasetDecl datasetDecl, 
    		HashMap<AqlCompiledIndexDecl, List<Pair<String, Integer>>> foundIndexExprs,
            String fieldName, int varIndex, boolean includePrimaryIndex) {
    	AqlCompiledIndexDecl primaryIndexDecl = DatasetUtils.getPrimaryIndex(datasetDecl);
    	List<String> primaryIndexFields = primaryIndexDecl.getFieldExprs();    	
        List<AqlCompiledIndexDecl> indexCandidates = DatasetUtils.findSecondaryIndexesByOneOfTheKeys(datasetDecl, fieldName);
        // Check whether the primary index is a candidate. If so, add it to the list.
        if (includePrimaryIndex && primaryIndexFields.contains(fieldName)) {
            if (indexCandidates == null) {
                indexCandidates = new ArrayList<AqlCompiledIndexDecl>(1);
            }
            indexCandidates.add(primaryIndexDecl);
        }
        // No index candidates for fieldName.
        if (indexCandidates == null) {
        	return false;
        }
        // Go through the candidates and fill foundIndexExprs.
        for (AqlCompiledIndexDecl index : indexCandidates) {
        	List<Pair<String, Integer>> psi = foundIndexExprs.get(index);
        	if (psi == null) {
        		psi = new ArrayList<Pair<String, Integer>>();
        		foundIndexExprs.put(index, psi);
        	}
        	psi.add(new Pair<String, Integer>(fieldName, varIndex));
        }
        return true;
    }

    protected int findVarInOutComparedVars(LogicalVariable var, ArrayList<LogicalVariable> outComparedVars) {
    	int outVarIndex = 0;
    	while (outVarIndex < outComparedVars.size()) {
    		if (var == outComparedVars.get(outVarIndex)) {
    			return outVarIndex;
    		}
    		outVarIndex++;
    	}
    	return -1;
    }
    
    /**
     * Returns the field name corresponding to the assigned variable at varIndex.
     * Returns null if the expr at varIndex is not a field access function.
     * 
     * @param assign
     * @param recordType
     * @param varIndex
     * @return
     */
    protected String getFieldNameOfFieldAccess(AssignOperator assign, ARecordType recordType, int varIndex) {
    	// Get expression corresponding to var at varIndex.
    	AbstractLogicalExpression assignExpr = (AbstractLogicalExpression) assign.getExpressions()
    			.get(varIndex).getValue();
    	if (assignExpr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
    		return null;
    	}
    	// Analyze the assign op to get the field name
    	// corresponding to the field being assigned at varIndex.
    	AbstractFunctionCallExpression assignFuncExpr = (AbstractFunctionCallExpression) assignExpr;
    	FunctionIdentifier assignFuncIdent = assignFuncExpr.getFunctionIdentifier();
    	if (assignFuncIdent == AsterixBuiltinFunctions.FIELD_ACCESS_BY_NAME) {
    		ILogicalExpression nameArg = assignFuncExpr.getArguments().get(1).getValue();
    		if (nameArg.getExpressionTag() != LogicalExpressionTag.CONSTANT) {
    			return null;
    		}
    		ConstantExpression constExpr = (ConstantExpression) nameArg;
    		return ((AString) ((AsterixConstantValue) constExpr.getValue()).getObject()).getStringValue();
    	} else if (assignFuncIdent == AsterixBuiltinFunctions.FIELD_ACCESS_BY_INDEX) {
    		ILogicalExpression idxArg = assignFuncExpr.getArguments().get(1).getValue();
    		if (idxArg.getExpressionTag() != LogicalExpressionTag.CONSTANT) {
    			return null;
    		}
    		ConstantExpression constExpr = (ConstantExpression) idxArg;
    		int fieldIndex = ((AInt32) ((AsterixConstantValue) constExpr.getValue()).getObject()).getIntegerValue();
    		return recordType.getFieldNames()[fieldIndex];
    	}
    	return null;
    }
    
    protected static List<Object> primaryIndexTypes(AqlCompiledMetadataDeclarations metadata,
            AqlCompiledDatasetDecl ddecl, IAType itemType) {
        List<Object> types = new ArrayList<Object>();
        List<Triple<IEvaluatorFactory, ScalarFunctionCallExpression, IAType>> partitioningFunctions = DatasetUtils
                .getPartitioningFunctions(ddecl);
        for (Triple<IEvaluatorFactory, ScalarFunctionCallExpression, IAType> t : partitioningFunctions) {
            types.add(t.third);
        }
        types.add(itemType);
        return types;
    }

}

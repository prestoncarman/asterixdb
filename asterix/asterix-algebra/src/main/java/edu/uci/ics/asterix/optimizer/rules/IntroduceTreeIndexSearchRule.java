package edu.uci.ics.asterix.optimizer.rules;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.asterix.metadata.declared.AqlCompiledDatasetDecl;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledIndexDecl;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledMetadataDeclarations;
import edu.uci.ics.asterix.metadata.utils.DatasetUtils;
import edu.uci.ics.asterix.om.base.AString;
import edu.uci.ics.asterix.om.constants.AsterixConstantValue;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.base.IEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import edu.uci.ics.hyracks.algebricks.core.utils.Pair;
import edu.uci.ics.hyracks.algebricks.core.utils.Triple;

public abstract class IntroduceTreeIndexSearchRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        return false;
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

    protected static ConstantExpression mkStrConstExpr(String str) {
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

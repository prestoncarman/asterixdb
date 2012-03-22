package edu.uci.ics.asterix.optimizer.rules.am;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import edu.uci.ics.asterix.common.functions.FunctionArgumentsConstants;
import edu.uci.ics.asterix.common.functions.FunctionUtils;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledDatasetDecl;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledIndexDecl;
import edu.uci.ics.asterix.om.base.AInt32;
import edu.uci.ics.asterix.om.constants.AsterixConstantValue;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.UnnestingFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;

public class WordInvertedIndexAccessMethod implements IAccessMethod {

    private static List<FunctionIdentifier> funcIdents = new ArrayList<FunctionIdentifier>();
    static {
        funcIdents.add(AsterixBuiltinFunctions.CONTAINS);
    }
    
    public static WordInvertedIndexAccessMethod INSTANCE = new WordInvertedIndexAccessMethod();
    
    @Override
    public List<FunctionIdentifier> getOptimizableFunctions() {
       return funcIdents;
    }

    @Override
    public boolean analyzeFuncExprArgs(AbstractFunctionCallExpression funcExpr, AccessMethodAnalysisContext analysisCtx) {
        return AccessMethodUtils.analyzeFuncExprArgsForOneConstAndVar(funcExpr, analysisCtx);
    }

    @Override
    public boolean matchAllIndexExprs() {
        return true;
    }

    @Override
    public boolean matchPrefixIndexExprs() {
        return false;
    }

    @Override
    public boolean applyPlanTransformation(Mutable<ILogicalOperator> selectRef, Mutable<ILogicalOperator> assignRef,
            Mutable<ILogicalOperator> dataSourceScanRef, AqlCompiledDatasetDecl datasetDecl, ARecordType recordType,
            AqlCompiledIndexDecl chosenIndex, AccessMethodAnalysisContext analysisCtx, IOptimizationContext context)
            throws AlgebricksException {
        System.out.println("READY FOR REWRITE");
        // TODO: Think more deeply about where this is used for inverted indexes, and what composite keys should mean.
        // For now we are assuming a single secondary index key.
        //int numSecondaryKeys = chosenIndex.getFieldExprs().size();
        int numSecondaryKeys = 1;
        
        // TODO: We can probably do something smarter here based on selectivity.
        // Pick the first expr optimizable by this index.
        List<Integer> indexExprs = analysisCtx.getIndexExprs(chosenIndex);
        int firstExprIndex = indexExprs.get(0);
        
        DataSourceScanOperator dataSourceScan = (DataSourceScanOperator) dataSourceScanRef.getValue();
        // List of arguments to be passed into an unnest.
        // This logical rewrite rule, and the corresponding runtime op generated in the jobgen 
        // have a contract as to what goes into these arguments.
        // Here, we put the name of the chosen index, the type of index, the name of the dataset, 
        // the number of secondary-index keys, and the variable references corresponding to the secondary-index search keys.
        ArrayList<Mutable<ILogicalExpression>> secondaryIndexFuncArgs = new ArrayList<Mutable<ILogicalExpression>>();
        secondaryIndexFuncArgs.add(new MutableObject<ILogicalExpression>(AccessMethodUtils.createStringConstant(chosenIndex.getIndexName())));
        secondaryIndexFuncArgs.add(new MutableObject<ILogicalExpression>(AccessMethodUtils.createStringConstant(FunctionArgumentsConstants.WORD_INVERTED_INDEX_INDEX)));
        secondaryIndexFuncArgs.add(new MutableObject<ILogicalExpression>(AccessMethodUtils.createStringConstant(datasetDecl.getName())));
        secondaryIndexFuncArgs.add(new MutableObject<ILogicalExpression>(new ConstantExpression(new AsterixConstantValue(
                new AInt32(numSecondaryKeys)))));
        // Here we generate vars and funcs for assigning the secondary-index keys to be fed into the secondary-index search.
        // List of variables for the assign.
        ArrayList<LogicalVariable> keyVarList = new ArrayList<LogicalVariable>();
        // List of expressions for the assign.
        ArrayList<Mutable<ILogicalExpression>> keyExprList = new ArrayList<Mutable<ILogicalExpression>>();
        // For now we are assuming a single secondary index key.
        // Add a variable and its expr to the lists which will be passed into an assign op.
        LogicalVariable keyVar = context.newVar();
        keyVarList.add(keyVar);
        keyExprList.add(new MutableObject<ILogicalExpression>(new ConstantExpression(analysisCtx.matchedFuncExprs.get(firstExprIndex).getConstVal())));
        Mutable<ILogicalExpression> keyVarRef = new MutableObject<ILogicalExpression>(
                new VariableReferenceExpression(keyVar));
        secondaryIndexFuncArgs.add(keyVarRef);

        // Assign operator that sets the secondary-index search-key fields.
        AssignOperator assignSearchKeys = new AssignOperator(keyVarList, keyExprList);
        // Input to this assign is the EmptyTupleSource (which the dataSourceScan also must have had as input).
        assignSearchKeys.getInputs().add(dataSourceScan.getInputs().get(0));
        assignSearchKeys.setExecutionMode(dataSourceScan.getExecutionMode());

        // This is the logical representation of our secondary-index search.
        // An index search is expressed logically as an unnest over an index-search function.
        IFunctionInfo secondaryIndexSearch = FunctionUtils.getFunctionInfo(AsterixBuiltinFunctions.INDEX_SEARCH);
        UnnestingFunctionCallExpression invIndexSearchFun = new UnnestingFunctionCallExpression(secondaryIndexSearch, secondaryIndexFuncArgs);
        invIndexSearchFun.setReturnsUniqueValues(true);

        // Generate the rest of the upstream plan which feeds the search results into the primary index.
        List<LogicalVariable> primaryIndexVars = dataSourceScan.getVariables();
        List<Object> secondaryIndexTypes = AccessMethodUtils.getSecondaryIndexTypes(datasetDecl, chosenIndex, recordType);
        UnnestMapOperator primaryIndexUnnestMap = AccessMethodUtils.createPrimaryIndexUnnestMap(datasetDecl, recordType,
                primaryIndexVars, chosenIndex, numSecondaryKeys, secondaryIndexTypes, invIndexSearchFun, assignSearchKeys,
                context, true);
        // Replace the datasource scan with the new plan rooted at primaryIndexUnnestMap.
        dataSourceScanRef.setValue(primaryIndexUnnestMap);
        return false;
    }
}

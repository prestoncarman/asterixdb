package edu.uci.ics.asterix.optimizer.rules.am;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import edu.uci.ics.asterix.common.functions.FunctionArgumentsConstants;
import edu.uci.ics.asterix.common.functions.FunctionUtils;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledDatasetDecl;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledIndexDecl;
import edu.uci.ics.asterix.om.base.AInt32;
import edu.uci.ics.asterix.om.base.AOrderedList;
import edu.uci.ics.asterix.om.base.AString;
import edu.uci.ics.asterix.om.base.IAObject;
import edu.uci.ics.asterix.om.constants.AsterixConstantValue;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.UnnestingFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
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
        // For matching similarity-check functions. For example similarity-jaccard-check returns a list of two items,
        // and the where condition will get the first list-item and check whether it evaluates to true. 
        // There may or may not be an explicit check on list[0] == true. If there is we match the EQ function, otherwise GET_ITEM.
        funcIdents.add(AlgebricksBuiltinFunctions.EQ);
        funcIdents.add(AsterixBuiltinFunctions.GET_ITEM);        
    }
    
    // These function identifiers are matched in this AM's analyzeFuncExprArgs(), and are not visible to the outside driver (IntroduceAccessMethodSearchRule).
    private static HashSet<FunctionIdentifier> secondLevelFuncIdents = new HashSet<FunctionIdentifier>();
    static {
        secondLevelFuncIdents.add(AsterixBuiltinFunctions.SIMILARITY_JACCARD_CHECK);
    }
    
    public static WordInvertedIndexAccessMethod INSTANCE = new WordInvertedIndexAccessMethod();
    
    @Override
    public List<FunctionIdentifier> getOptimizableFunctions() {
       return funcIdents;
    }

    @Override
    public boolean analyzeFuncExprArgs(AbstractFunctionCallExpression funcExpr, List<AssignOperator> assigns, AccessMethodAnalysisContext analysisCtx) {
        if (funcExpr.getFunctionIdentifier() == AsterixBuiltinFunctions.CONTAINS) {
            return AccessMethodUtils.analyzeFuncExprArgsForOneConstAndVar(funcExpr, analysisCtx);
        }
        if (funcExpr.getFunctionIdentifier() == AsterixBuiltinFunctions.GET_ITEM) {
            ILogicalExpression arg1 = funcExpr.getArguments().get(0).getValue();
            ILogicalExpression arg2 = funcExpr.getArguments().get(1).getValue();
            // The second arg is the item index to be accessed. It must be a constant.
            if (arg2.getExpressionTag() != LogicalExpressionTag.CONSTANT) {
                return false;
            }
            // The first arg is the referenced variable, whose origination function we must track in the assign.
            if (arg1.getExpressionTag() != LogicalExpressionTag.VARIABLE) {
                return false;
            }
            VariableReferenceExpression varRefExpr = (VariableReferenceExpression) arg1;
            // Try to find variable ref expr in assign.
            AssignOperator firstAssign = assigns.get(0);
            List<LogicalVariable> assignVars = firstAssign.getVariables();
            List<Mutable<ILogicalExpression>> assignExprs = firstAssign.getExpressions();
            for (int i = 0; i < assignVars.size(); i++) {
                LogicalVariable var = assignVars.get(i);
                if (var == varRefExpr.getVariableReference()) {
                    // We've matched the variable in the first assign. Now analyze the originating function.
                    ILogicalExpression matchedExpr = assignExprs.get(i).getValue();
                    if (matchedExpr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
                        return false;
                    }
                    AbstractFunctionCallExpression matchedFuncExpr = (AbstractFunctionCallExpression) matchedExpr;
                    // Check that this function is optimizable by this access method.
                    if (!secondLevelFuncIdents.contains(matchedFuncExpr.getFunctionIdentifier())) {
                        return false;
                    }
                    return AccessMethodUtils.analyzeSimilarityCheckFuncExprArgs(matchedFuncExpr, analysisCtx);
                }
            }
        }
        return false;
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
        // TODO: Think more deeply about where this is used for inverted indexes, and what composite keys should mean.
        // For now we are assuming a single secondary index key.
        //int numSecondaryKeys = chosenIndex.getFieldExprs().size();
        int numSecondaryKeys = 1;
        
        // TODO: We can probably do something smarter here based on selectivity.
        // Pick the first expr optimizable by this index.
        List<Integer> indexExprs = analysisCtx.getIndexExprs(chosenIndex);
        int firstExprIndex = indexExprs.get(0);
        OptimizableBinaryFuncExpr optFuncExpr = analysisCtx.matchedFuncExprs.get(firstExprIndex);
        
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
        // Add function-specific args such as search modifier, and possibly a similarity threshold.
        addFunctionSpecificArgs(optFuncExpr, secondaryIndexFuncArgs);
        secondaryIndexFuncArgs.add(new MutableObject<ILogicalExpression>(new ConstantExpression(new AsterixConstantValue(
                new AInt32(numSecondaryKeys)))));
        // Here we generate vars and funcs for assigning the secondary-index keys to be fed into the secondary-index search.
        // List of variables for the assign.
        ArrayList<LogicalVariable> keyVarList = new ArrayList<LogicalVariable>();
        // List of expressions for the assign.
        ArrayList<Mutable<ILogicalExpression>> keyExprList = new ArrayList<Mutable<ILogicalExpression>>();
        // Add key vars and exprs to argument list.
        addKeyVarsAndExprs(optFuncExpr, keyVarList, keyExprList, secondaryIndexFuncArgs, context);

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
        List<Object> secondaryIndexTypes = AccessMethodUtils.getSecondaryIndexTypes(datasetDecl, chosenIndex, recordType, true);
        UnnestMapOperator primaryIndexUnnestMap = AccessMethodUtils.createPrimaryIndexUnnestMap(datasetDecl, recordType,
                primaryIndexVars, chosenIndex, numSecondaryKeys, secondaryIndexTypes, invIndexSearchFun, assignSearchKeys,
                context, true, true);
        // Replace the datasource scan with the new plan rooted at primaryIndexUnnestMap.
        dataSourceScanRef.setValue(primaryIndexUnnestMap);
        return true;
    }
    
    private void addFunctionSpecificArgs(OptimizableBinaryFuncExpr optFuncExpr, ArrayList<Mutable<ILogicalExpression>> secondaryIndexFuncArgs) {
        if (optFuncExpr.getFuncExpr().getFunctionIdentifier() == AsterixBuiltinFunctions.CONTAINS) {
            // Value 0 represents a conjunctive search modifier.
            secondaryIndexFuncArgs.add(new MutableObject<ILogicalExpression>(new ConstantExpression(new AsterixConstantValue(
                    new AString("CONJUNCTIVE")))));
            // We add this dummy value, so that we can get the the key arguments starting from a fixed index.
            secondaryIndexFuncArgs.add(new MutableObject<ILogicalExpression>(new ConstantExpression(new AsterixConstantValue(
                    new AString("")))));
        }
        if (optFuncExpr.getFuncExpr().getFunctionIdentifier() == AsterixBuiltinFunctions.SIMILARITY_JACCARD_CHECK) {
            // Value 1 represents a jaccard search modifier.
            secondaryIndexFuncArgs.add(new MutableObject<ILogicalExpression>(new ConstantExpression(new AsterixConstantValue(
                    new AString("JACCARD")))));
            // Add the similarity threshold.
            OptimizableTernaryFuncExpr ternOptFuncExpr = (OptimizableTernaryFuncExpr) optFuncExpr;
            secondaryIndexFuncArgs.add(new MutableObject<ILogicalExpression>(new ConstantExpression(ternOptFuncExpr.getSecondConstVal())));
        }
    }

    private void addKeyVarsAndExprs(OptimizableBinaryFuncExpr optFuncExpr, ArrayList<LogicalVariable> keyVarList, ArrayList<Mutable<ILogicalExpression>> keyExprList, ArrayList<Mutable<ILogicalExpression>> secondaryIndexFuncArgs, IOptimizationContext context) throws AlgebricksException {
        if (optFuncExpr.getFuncExpr().getFunctionIdentifier() == AsterixBuiltinFunctions.CONTAINS) {
            // For now we are assuming a single secondary index key.
            // Add a variable and its expr to the lists which will be passed into an assign op.
            LogicalVariable keyVar = context.newVar();
            keyVarList.add(keyVar);
            Mutable<ILogicalExpression> keyVarRef = new MutableObject<ILogicalExpression>(
                    new VariableReferenceExpression(keyVar));
            secondaryIndexFuncArgs.add(keyVarRef);
            keyExprList.add(new MutableObject<ILogicalExpression>(new ConstantExpression(optFuncExpr.getConstVal())));
            return;
        }
        if (optFuncExpr.getFuncExpr().getFunctionIdentifier() == AsterixBuiltinFunctions.SIMILARITY_JACCARD_CHECK) {
            // TODO: We expect an already constant-folded list of tokens.
            // However, the Hyracks operator expects an untokenized string, so we reconstruct it here.
            // We should allow the Hyracks inverted-index operator to accept lists.
            LogicalVariable keyVar = context.newVar();
            keyVarList.add(keyVar);
            Mutable<ILogicalExpression> keyVarRef = new MutableObject<ILogicalExpression>(
                    new VariableReferenceExpression(keyVar));
            secondaryIndexFuncArgs.add(keyVarRef);
            
            AsterixConstantValue constVal = (AsterixConstantValue) optFuncExpr.getConstVal();
            IAObject obj = constVal.getObject();
            if (obj.getType().getTypeTag() != ATypeTag.ORDEREDLIST) {
                throw new AlgebricksException("Expected type ORDEREDLIST.");
            }
            AOrderedList tokenList = (AOrderedList) obj;
            // TODO: Currently, we only accept strings here.
            StringBuilder searchString = new StringBuilder();
            for (int i = 0; i < tokenList.size(); i++) {
                AString item = (AString) tokenList.getItem(i);
                searchString.append(item.getStringValue());
                searchString.append(" ");
            }
            searchString.deleteCharAt(searchString.length() - 1);
            keyExprList.add(new MutableObject<ILogicalExpression>(new ConstantExpression(new AsterixConstantValue(new AString(searchString.toString())))));
        }
    }
}

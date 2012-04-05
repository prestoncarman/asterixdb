package edu.uci.ics.asterix.optimizer.rules.am;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import edu.uci.ics.asterix.common.functions.FunctionArgumentsConstants;
import edu.uci.ics.asterix.common.functions.FunctionUtils;
import edu.uci.ics.asterix.dataflow.data.common.ListEditDistanceSearchModifierFactory;
import edu.uci.ics.asterix.formats.nontagged.AqlBinaryComparatorFactoryProvider;
import edu.uci.ics.asterix.formats.nontagged.AqlBinaryTokenizerFactoryProvider;
import edu.uci.ics.asterix.formats.nontagged.AqlTypeTraitProvider;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledDatasetDecl;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledIndexDecl;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledIndexDecl.IndexKind;
import edu.uci.ics.asterix.om.base.AFloat;
import edu.uci.ics.asterix.om.base.AInt32;
import edu.uci.ics.asterix.om.base.AString;
import edu.uci.ics.asterix.om.base.IACollection;
import edu.uci.ics.asterix.om.base.IAObject;
import edu.uci.ics.asterix.om.constants.AsterixConstantValue;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.types.AOrderedListType;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.AUnorderedListType;
import edu.uci.ics.asterix.om.types.AbstractCollectionType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.translator.DmlTranslator.CompiledCreateIndexStatement;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.UnnestingFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.DataSourceScanOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.OrderOperator.IOrder.OrderKind;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedIndexSearchModifierFactory;
import edu.uci.ics.hyracks.storage.am.invertedindex.searchmodifiers.ConjunctiveSearchModifierFactory;
import edu.uci.ics.hyracks.storage.am.invertedindex.searchmodifiers.EditDistanceSearchModifierFactory;
import edu.uci.ics.hyracks.storage.am.invertedindex.searchmodifiers.JaccardSearchModifierFactory;
import edu.uci.ics.hyracks.storage.am.invertedindex.tokenizers.IBinaryTokenizerFactory;

public class InvertedIndexAccessMethod implements IAccessMethod {

    // Enum describing the search modifier type. Used for passing info to jobgen.
    public static enum SearchModifierType {
        CONJUNCTIVE,
        JACCARD,
        EDIT_DISTANCE,
        INVALID
    }
    
    private static List<FunctionIdentifier> funcIdents = new ArrayList<FunctionIdentifier>();
    static {
        funcIdents.add(AsterixBuiltinFunctions.CONTAINS);        
        // For matching similarity-check functions. For example similarity-jaccard-check returns a list of two items,
        // and the where condition will get the first list-item and check whether it evaluates to true. 
        // There may or may not be an explicit check on list[0] == true. If there is we match the EQ function, otherwise GET_ITEM.
        // TODO: Make RE work as well.
        //funcIdents.add(AlgebricksBuiltinFunctions.EQ);
        funcIdents.add(AsterixBuiltinFunctions.GET_ITEM);
    }
    
    // These function identifiers are matched in this AM's analyzeFuncExprArgs(), 
    // and are not visible to the outside driver (IntroduceAccessMethodSearchRule).
    private static HashSet<FunctionIdentifier> secondLevelFuncIdents = new HashSet<FunctionIdentifier>();
    static {
        secondLevelFuncIdents.add(AsterixBuiltinFunctions.SIMILARITY_JACCARD_CHECK);
        secondLevelFuncIdents.add(AsterixBuiltinFunctions.EDIT_DISTANCE_CHECK);
    }
    
    public static InvertedIndexAccessMethod INSTANCE = new InvertedIndexAccessMethod();
    
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
        if (chosenIndex.getKind() == IndexKind.WORD_INVIX) {
            secondaryIndexFuncArgs.add(new MutableObject<ILogicalExpression>(AccessMethodUtils.createStringConstant(FunctionArgumentsConstants.WORD_INVERTED_INDEX)));
        } else if (chosenIndex.getKind() == IndexKind.NGRAM_INVIX) {
            secondaryIndexFuncArgs.add(new MutableObject<ILogicalExpression>(AccessMethodUtils.createStringConstant(FunctionArgumentsConstants.NGRAM_INVERTED_INDEX)));
        }
        secondaryIndexFuncArgs.add(new MutableObject<ILogicalExpression>(AccessMethodUtils.createStringConstant(datasetDecl.getName())));
        // Add function-specific args such as search modifier, and possibly a similarity threshold.
        addFunctionSpecificArgs(optFuncExpr, secondaryIndexFuncArgs);
        // Add the type of the constant in the optFuncExpr.
        addKeyConstantType(optFuncExpr, secondaryIndexFuncArgs);
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
    
    private void addKeyConstantType(OptimizableBinaryFuncExpr optFuncExpr, ArrayList<Mutable<ILogicalExpression>> secondaryIndexFuncArgs) throws AlgebricksException {
        // Pass on type of constant value to pick the right tokenizer in jobgen.
        AsterixConstantValue constVal = (AsterixConstantValue) optFuncExpr.getConstVal();
        IAObject obj = constVal.getObject();
        ATypeTag typeTag = obj.getType().getTypeTag();
        if (typeTag != ATypeTag.ORDEREDLIST && typeTag != ATypeTag.STRING) {
            throw new AlgebricksException("Only ordered lists and string types supported.");
        }
        secondaryIndexFuncArgs.add(new MutableObject<ILogicalExpression>(new ConstantExpression(new AsterixConstantValue(new AInt32(typeTag.ordinal())))));
    }
    
    private void addFunctionSpecificArgs(OptimizableBinaryFuncExpr optFuncExpr, ArrayList<Mutable<ILogicalExpression>> secondaryIndexFuncArgs) {
        if (optFuncExpr.getFuncExpr().getFunctionIdentifier() == AsterixBuiltinFunctions.CONTAINS) {
            // Value 0 represents a conjunctive search modifier.
            secondaryIndexFuncArgs.add(new MutableObject<ILogicalExpression>(new ConstantExpression(new AsterixConstantValue(
                    new AInt32(SearchModifierType.CONJUNCTIVE.ordinal())))));
            // We add this dummy value, so that we can get the the key arguments starting from a fixed index.
            secondaryIndexFuncArgs.add(new MutableObject<ILogicalExpression>(new ConstantExpression(new AsterixConstantValue(
                    new AInt32(-1)))));
        }
        if (optFuncExpr.getFuncExpr().getFunctionIdentifier() == AsterixBuiltinFunctions.SIMILARITY_JACCARD_CHECK) {
            // Value 1 represents a jaccard search modifier.
            secondaryIndexFuncArgs.add(new MutableObject<ILogicalExpression>(new ConstantExpression(new AsterixConstantValue(
                    new AInt32(SearchModifierType.JACCARD.ordinal())))));
            // Add the similarity threshold.
            OptimizableTernaryFuncExpr ternOptFuncExpr = (OptimizableTernaryFuncExpr) optFuncExpr;
            secondaryIndexFuncArgs.add(new MutableObject<ILogicalExpression>(new ConstantExpression(ternOptFuncExpr.getSecondConstVal())));
        }
        if (optFuncExpr.getFuncExpr().getFunctionIdentifier() == AsterixBuiltinFunctions.EDIT_DISTANCE_CHECK) {
            // Value 1 represents an edit distance search modifier.
            secondaryIndexFuncArgs.add(new MutableObject<ILogicalExpression>(new ConstantExpression(new AsterixConstantValue(
                    new AInt32(SearchModifierType.EDIT_DISTANCE.ordinal())))));
            // Add the similarity threshold.
            OptimizableTernaryFuncExpr ternOptFuncExpr = (OptimizableTernaryFuncExpr) optFuncExpr;
            secondaryIndexFuncArgs.add(new MutableObject<ILogicalExpression>(new ConstantExpression(ternOptFuncExpr.getSecondConstVal())));
        }
    }

    private void addKeyVarsAndExprs(OptimizableBinaryFuncExpr optFuncExpr, ArrayList<LogicalVariable> keyVarList, ArrayList<Mutable<ILogicalExpression>> keyExprList, ArrayList<Mutable<ILogicalExpression>> secondaryIndexFuncArgs, IOptimizationContext context) throws AlgebricksException {
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

    @Override
    public boolean exprIsOptimizable(AqlCompiledIndexDecl index, OptimizableBinaryFuncExpr expr) {
        if (expr.getFuncExpr().getFunctionIdentifier() == AsterixBuiltinFunctions.EDIT_DISTANCE_CHECK) {
            // Check for panic.
            // TODO: Panic also depends on prePost which is currently hardcoded to be true.
            OptimizableTernaryFuncExpr ternaryExpr = (OptimizableTernaryFuncExpr) expr;            
            AsterixConstantValue strConstVal = (AsterixConstantValue) expr.getConstVal();
            AsterixConstantValue intConstVal = (AsterixConstantValue) ternaryExpr.getSecondConstVal();
            IAObject strObj = strConstVal.getObject();
            IAObject intObj = intConstVal.getObject();
            AInt32 edThresh = (AInt32) intObj;
            int mergeThreshold = 0;
            // We can only optimize edit distance on strings using an ngram index.
            if (strObj.getType().getTypeTag() == ATypeTag.STRING && index.getKind() == IndexKind.NGRAM_INVIX) {
                AString astr = (AString) strObj;                    
                // Compute merge threshold.
                mergeThreshold = (astr.getStringValue().length() + index.getGramLength() - 1)
                        - edThresh.getIntegerValue() * index.getGramLength();
            }
            // We can only optimize edit distance on lists using a word index.
            if ((strObj.getType().getTypeTag() == ATypeTag.ORDEREDLIST || strObj.getType().getTypeTag() == ATypeTag.UNORDEREDLIST)
                    && index.getKind() == IndexKind.WORD_INVIX) {
                IACollection alist = (IACollection) strObj;        
                // Compute merge threshold.
                mergeThreshold = alist.size() - edThresh.getIntegerValue();
            }
            if (mergeThreshold <= 0) {
                // We cannot use index to optimize expr.
                return false;
            }
            return true;
        }
        // TODO: We need more checking. Also need to check the gram length, prePost, etc.
        if (expr.getFuncExpr().getFunctionIdentifier() == AsterixBuiltinFunctions.SIMILARITY_JACCARD_CHECK) {
            // Check the tokenization function of the non-constant func arg to see if it fits the concrete index type.
            ILogicalExpression arg1 = expr.getFuncExpr().getArguments().get(0).getValue();
            ILogicalExpression arg2 = expr.getFuncExpr().getArguments().get(1).getValue();
            ILogicalExpression nonConstArg = null;
            if (arg1.getExpressionTag() != LogicalExpressionTag.CONSTANT) {
                nonConstArg = arg1;
            } else {
                nonConstArg = arg2;
            }
            if (nonConstArg.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                AbstractFunctionCallExpression nonConstfuncExpr = (AbstractFunctionCallExpression) nonConstArg;
                // We can use this index if the tokenization function matches the index type.
                if (nonConstfuncExpr.getFunctionIdentifier() == AsterixBuiltinFunctions.WORD_TOKENS &&
                        index.getKind() == IndexKind.WORD_INVIX) {
                    return true;
                }
                if (nonConstfuncExpr.getFunctionIdentifier() == AsterixBuiltinFunctions.GRAM_TOKENS &&
                        index.getKind() == IndexKind.NGRAM_INVIX) {
                    return true;
                }
            }
            // The non-constant arg is not a function call. Perhaps a variable?
            if (nonConstArg.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
                // In this case we only allow word inverted indexes, since they are used to index list types.
                if (index.getKind() == IndexKind.WORD_INVIX) {
                    return true;
                }
            }
        }
        if (expr.getFuncExpr().getFunctionIdentifier() == AsterixBuiltinFunctions.CONTAINS) {
            // TODO: No further analysis for now. Think about how to support keyword queries with an ngram index.
            return true;
        }
        return false;
    }
    
    public static IBinaryComparatorFactory getTokenBinaryComparatorFactory(IAType keyType) throws AlgebricksException {
        IAType type = keyType;
        ATypeTag typeTag = keyType.getTypeTag();        
        // Extract item type from list.
        if (typeTag == ATypeTag.UNORDEREDLIST || typeTag == ATypeTag.ORDEREDLIST) {
            AbstractCollectionType listType = (AbstractCollectionType) keyType;
            if (!listType.isTyped()) {
                throw new AlgebricksException("Cannot build an inverted index on untyped lists.)");
            }
            type = listType.getItemType();
        }
        // Ignore case for string types.
        return AqlBinaryComparatorFactoryProvider.INSTANCE.getBinaryComparatorFactory(
                type, OrderKind.ASC, true);
    }
    
    public static ITypeTraits getTokenTypeTrait(IAType keyType) throws AlgebricksException {
        IAType type = keyType;
        ATypeTag typeTag = keyType.getTypeTag();
        // Extract item type from list.
        if (typeTag == ATypeTag.UNORDEREDLIST) {
            AUnorderedListType ulistType = (AUnorderedListType) keyType;
            if (!ulistType.isTyped()) {
                throw new AlgebricksException("Cannot build an inverted index on untyped lists.)");
            }
            type = ulistType.getItemType();
        }
        if (typeTag == ATypeTag.ORDEREDLIST) {
            AOrderedListType olistType = (AOrderedListType) keyType;
            if (!olistType.isTyped()) {
                throw new AlgebricksException("Cannot build an inverted index on untyped lists.)");
            }
            type = olistType.getItemType();
        }
        return AqlTypeTraitProvider.INSTANCE.getTypeTrait(type);
    }
    
    public static IBinaryTokenizerFactory getBinaryTokenizerFactory(SearchModifierType searchModifierType, ATypeTag searchKeyType, AqlCompiledIndexDecl index)
            throws AlgebricksException {
        switch (index.getKind()) {
            case WORD_INVIX: {
                return AqlBinaryTokenizerFactoryProvider.INSTANCE.getWordTokenizerFactory(searchKeyType, false);
            }
            case NGRAM_INVIX: {
                // Make sure not to use pre- and postfixing for conjunctive searches.
                boolean prePost = (searchModifierType == SearchModifierType.CONJUNCTIVE) ? false : true;
                return AqlBinaryTokenizerFactoryProvider.INSTANCE.getNGramTokenizerFactory(searchKeyType,
                        index.getGramLength(), prePost, false);
            }
            default: {
                throw new AlgebricksException("Tokenizer not applicable to index kind '" + index.getKind() + "'.");
            }
        }
    }
    
    public static IBinaryTokenizerFactory getBinaryTokenizerFactory(ATypeTag keyType, CompiledCreateIndexStatement createIndexStmt)
            throws AlgebricksException {
        switch (createIndexStmt.getIndexType()) {
            case WORD_INVIX: {
                return AqlBinaryTokenizerFactoryProvider.INSTANCE.getWordTokenizerFactory(keyType, false);
            }
            case NGRAM_INVIX: {
                return AqlBinaryTokenizerFactoryProvider.INSTANCE.getNGramTokenizerFactory(keyType,
                        createIndexStmt.getGramLength(), true, false);
            }
            default: {
                throw new AlgebricksException("Tokenizer not applicable to index type '" + createIndexStmt.getIndexType() + "'.");
            }
        }
    }
    
    public static IInvertedIndexSearchModifierFactory getSearchModifierFactory(SearchModifierType searchModifierType, IAObject simThresh, AqlCompiledIndexDecl index) throws AlgebricksException {
        switch(searchModifierType) {
            case CONJUNCTIVE: {
                return new ConjunctiveSearchModifierFactory();
            }
            case JACCARD: {
                float jaccThresh = ((AFloat) simThresh).getFloatValue();
                return new JaccardSearchModifierFactory(jaccThresh);
            }
            case EDIT_DISTANCE: {
                int edThresh = ((AInt32) simThresh).getIntegerValue();
                switch (index.getKind()) {
                    case NGRAM_INVIX: {
                        // Edit distance on overlapping grams.
                        return new EditDistanceSearchModifierFactory(index.getGramLength(), edThresh);
                    }
                    case WORD_INVIX: {
                        // Edit distance on two lists. The list-elements are non-overlapping.
                        return new ListEditDistanceSearchModifierFactory(edThresh);
                    }
                    default: {
                        throw new AlgebricksException("Incompatible search modifier '" + searchModifierType + "' for index type '" + index.getKind() + "'");
                    }
                }
            }
            default: {
                throw new AlgebricksException("Unknown search modifier type '" + searchModifierType + "'.");
            }
        }
    }
}

package edu.uci.ics.asterix.optimizer.rules;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import edu.uci.ics.hyracks.algebricks.rewriter.util.JoinUtils;
import edu.uci.ics.asterix.algebra.operators.physical.BTreeSearchPOperator;
import edu.uci.ics.asterix.algebra.operators.physical.RTreeSearchPOperator;
import edu.uci.ics.asterix.algebra.operators.physical.WordInvertedIndexPOperator;
import edu.uci.ics.asterix.common.functions.FunctionArgumentsConstants;
import edu.uci.ics.asterix.metadata.declared.AqlMetadataProvider;
import edu.uci.ics.asterix.metadata.declared.AqlSourceId;
import edu.uci.ics.asterix.om.base.AString;
import edu.uci.ics.asterix.om.constants.AsterixConstantValue;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.OperatorAnnotations;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IMergeAggregationExpressionFactory;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IDataSourceIndex;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractOperatorWithNestedPlans;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.InnerJoinOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.LeftOuterJoinOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.UnnestMapOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.physical.ExternalGroupByPOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.physical.PreclusteredGroupByPOperator;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.NotImplementedException;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.PhysicalOptimizationConfig;
import edu.uci.ics.hyracks.algebricks.core.utils.Pair;

public class SetAsterixPhysicalOperatorsRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) throws AlgebricksException {
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (context.checkIfInDontApplySet(this, op)) {
            return false;
        }

        computeDefaultPhysicalOp(op, context);
        context.addToDontApplySet(this, op);
        return true;
    }

    private static void setPhysicalOperators(ILogicalPlan plan, IOptimizationContext context)
            throws AlgebricksException {
        for (Mutable<ILogicalOperator> root : plan.getRoots()) {
            computeDefaultPhysicalOp((AbstractLogicalOperator) root.getValue(), context);
        }
    }

    private static void computeDefaultPhysicalOp(AbstractLogicalOperator op, IOptimizationContext context)
            throws AlgebricksException {
        PhysicalOptimizationConfig physicalOptimizationConfig = context.getPhysicalOptimizationConfig();
        if (op.getOperatorTag().equals(LogicalOperatorTag.GROUP)) {
            GroupByOperator gby = (GroupByOperator) op;
            if (gby.getNestedPlans().size() == 1) {
                ILogicalPlan p0 = gby.getNestedPlans().get(0);
                if (p0.getRoots().size() == 1) {
                    Mutable<ILogicalOperator> r0 = p0.getRoots().get(0);
                    AggregateOperator aggOp = (AggregateOperator) r0.getValue();
                    boolean serializable = true;
                    for (Mutable<ILogicalExpression> exprRef : aggOp.getExpressions()) {
                        AbstractFunctionCallExpression expr = (AbstractFunctionCallExpression) exprRef.getValue();
                        if (!AsterixBuiltinFunctions.isAggregateFunctionSerializable(expr.getFunctionIdentifier())) {
                            serializable = false;
                            break;
                        }
                    }

                    if ((gby.getAnnotations().get(OperatorAnnotations.USE_HASH_GROUP_BY) == Boolean.TRUE || gby
                            .getAnnotations().get(OperatorAnnotations.USE_EXTERNAL_GROUP_BY) == Boolean.TRUE)) {
                        if (serializable) {
                            // if not serializable, use external group-by
                            int i = 0;
                            for (Mutable<ILogicalExpression> exprRef : aggOp.getExpressions()) {
                                AbstractFunctionCallExpression expr = (AbstractFunctionCallExpression) exprRef
                                        .getValue();
                                AggregateFunctionCallExpression serialAggExpr = AsterixBuiltinFunctions
                                        .makeSerializableAggregateFunctionExpression(expr.getFunctionIdentifier(),
                                                expr.getArguments());
                                aggOp.getExpressions().get(i).setValue(serialAggExpr);
                                i++;
                            }

                            ExternalGroupByPOperator externalGby = new ExternalGroupByPOperator(gby.getGroupByList(),
                                    physicalOptimizationConfig.getMaxFramesExternalGroupBy(),
                                    physicalOptimizationConfig.getExternalGroupByTableSize());
                            op.setPhysicalOperator(externalGby);
                            generateMergeAggregationExpressions(gby, context);
                        } else {
                            // if not serializable, use pre-clustered group-by
                            List<Pair<LogicalVariable, Mutable<ILogicalExpression>>> gbyList = gby.getGroupByList();
                            List<LogicalVariable> columnList = new ArrayList<LogicalVariable>(gbyList.size());
                            for (Pair<LogicalVariable, Mutable<ILogicalExpression>> p : gbyList) {
                                ILogicalExpression expr = p.second.getValue();
                                if (expr.getExpressionTag() == LogicalExpressionTag.VARIABLE) {
                                    VariableReferenceExpression varRef = (VariableReferenceExpression) expr;
                                    columnList.add(varRef.getVariableReference());
                                }
                            }
                            op.setPhysicalOperator(new PreclusteredGroupByPOperator(columnList));
                        }
                    }
                }
            }
        }
        if (op.getPhysicalOperator() == null) {
            switch (op.getOperatorTag()) {
                case INNERJOIN: {
                    JoinUtils.setJoinAlgorithmAndExchangeAlgo((InnerJoinOperator) op, context);
                    break;
                }
                case LEFTOUTERJOIN: {
                    JoinUtils.setJoinAlgorithmAndExchangeAlgo((LeftOuterJoinOperator) op, context);
                    break;
                }
                case UNNEST_MAP: {
                    UnnestMapOperator unnestMap = (UnnestMapOperator) op;
                    ILogicalExpression unnestExpr = unnestMap.getExpressionRef().getValue();
                    boolean notSet = true;
                    if (unnestExpr.getExpressionTag() == LogicalExpressionTag.FUNCTION_CALL) {
                        AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) unnestExpr;
                        FunctionIdentifier fid = f.getFunctionIdentifier();
                        if (fid == AsterixBuiltinFunctions.INDEX_SEARCH) {
                            notSet = false;
                            AqlMetadataProvider mp = (AqlMetadataProvider) context.getMetadataProvider();
                            ConstantExpression ce0 = (ConstantExpression) f.getArguments().get(0).getValue();
                            String indexId = ((AString) ((AsterixConstantValue) ce0.getValue()).getObject())
                                    .getStringValue();
                            ConstantExpression ce2 = (ConstantExpression) f.getArguments().get(2).getValue();
                            String datasetName = ((AString) ((AsterixConstantValue) ce2.getValue()).getObject())
                                    .getStringValue();
                            String dvName = mp.getMetadataDeclarations().getDataverseName();
                            AqlSourceId dataSourceId = new AqlSourceId(dvName, datasetName);
                            IDataSourceIndex<String, AqlSourceId> dsi = mp.findDataSourceIndex(indexId, dataSourceId);
                            if (dsi == null) {
                                throw new AlgebricksException("Could not find index " + indexId + " for dataset "
                                        + dataSourceId);
                            }
                            ConstantExpression ce1 = (ConstantExpression) f.getArguments().get(1).getValue();
                            String indexType = ((AString) ((AsterixConstantValue) ce1.getValue()).getObject())
                                    .getStringValue();
                            if (indexType == FunctionArgumentsConstants.BTREE_INDEX) {
                                op.setPhysicalOperator(new BTreeSearchPOperator(dsi));
                            } else if (indexType == FunctionArgumentsConstants.RTREE_INDEX) {
                                op.setPhysicalOperator(new RTreeSearchPOperator(dsi));
                            } else if (indexType == FunctionArgumentsConstants.WORD_INVERTED_INDEX ) {
                                op.setPhysicalOperator(new WordInvertedIndexPOperator(dsi));
                            } else if (indexType == FunctionArgumentsConstants.NGRAM_INVERTED_INDEX ) {
                                op.setPhysicalOperator(new WordInvertedIndexPOperator(dsi));
                            } else {
                                throw new NotImplementedException(indexType + " indexes are not implemented.");
                            }
                        }
                    }
                    if (notSet) {
                        throw new IllegalStateException();
                    }
                    break;
                }
            }
        }
        if (op.hasNestedPlans()) {
            AbstractOperatorWithNestedPlans nested = (AbstractOperatorWithNestedPlans) op;
            for (ILogicalPlan p : nested.getNestedPlans()) {
                setPhysicalOperators(p, context);
            }
        }
        for (Mutable<ILogicalOperator> opRef : op.getInputs()) {
            computeDefaultPhysicalOp((AbstractLogicalOperator) opRef.getValue(), context);
        }
    }

    private static void generateMergeAggregationExpressions(GroupByOperator gby, IOptimizationContext context)
            throws AlgebricksException {
        if (gby.getNestedPlans().size() != 1) {
            throw new AlgebricksException(
                    "External group-by currently works only for one nested plan with one root containing"
                            + "an aggregate and a nested-tuple-source.");
        }
        ILogicalPlan p0 = gby.getNestedPlans().get(0);
        if (p0.getRoots().size() != 1) {
            throw new AlgebricksException(
                    "External group-by currently works only for one nested plan with one root containing"
                            + "an aggregate and a nested-tuple-source.");
        }
        IMergeAggregationExpressionFactory mergeAggregationExpressionFactory = context
                .getMergeAggregationExpressionFactory();
        Mutable<ILogicalOperator> r0 = p0.getRoots().get(0);
        AggregateOperator aggOp = (AggregateOperator) r0.getValue();
        List<Mutable<ILogicalExpression>> aggFuncRefs = aggOp.getExpressions();
        int n = aggOp.getExpressions().size();
        List<Mutable<ILogicalExpression>> mergeExpressionRefs = new ArrayList<Mutable<ILogicalExpression>>();
        for (int i = 0; i < n; i++) {
            ILogicalExpression mergeExpr = mergeAggregationExpressionFactory.createMergeAggregation(aggFuncRefs.get(i)
                    .getValue(), context);
            mergeExpressionRefs.add(new MutableObject<ILogicalExpression>(mergeExpr));
        }
        aggOp.setMergeExpressions(mergeExpressionRefs);
    }

}
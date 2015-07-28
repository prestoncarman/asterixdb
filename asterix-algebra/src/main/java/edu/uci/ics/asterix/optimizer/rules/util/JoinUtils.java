/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.optimizer.rules.util;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import edu.uci.ics.asterix.algebra.operators.physical.interval.SortMergeIntervalJoinPOperator;
import edu.uci.ics.asterix.common.annotations.IntervalJoinExpressionAnnotation;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IExpressionAnnotation;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.physical.AbstractJoinPOperator.JoinPartitioningType;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.physical.HybridHashJoinPOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.physical.NLJoinPOperator;

public class JoinUtils {

    public static void setJoinAlgorithmAndExchangeAlgo(AbstractBinaryJoinOperator op, IOptimizationContext context)
            throws AlgebricksException {
        List<LogicalVariable> sideLeft = new LinkedList<LogicalVariable>();
        List<LogicalVariable> sideRight = new LinkedList<LogicalVariable>();
        List<LogicalVariable> varsLeft = op.getInputs().get(0).getValue().getSchema();
        List<LogicalVariable> varsRight = op.getInputs().get(1).getValue().getSchema();
        AbstractFunctionCallExpression fexp = (AbstractFunctionCallExpression) op.getCondition().getValue();
        if (isIntervalJoinCondition(fexp, varsLeft, varsRight, sideLeft, sideRight)) {
            IntervalJoinExpressionAnnotation ijea = getIntervalJoinAnnotation(fexp);
            if (ijea == null) {
                // Use default join method.
                return;
            }
            if (ijea.isMergeJoin()) {
                // Sort Merge.
                System.err.println("Interval Join - Merge");
                setSortMergeIntervalJoinOp(op, context);
            } else if (ijea.isIopJoin()) {
                // Overlapping Interval Partition.
                System.err.println("Interval Join - IOP");
            } else if (ijea.isSpatialJoin()) {
                // Spatial Partition.
                System.err.println("Interval Join - Spatial Partitioning");
            }
        }
    }

    private static IntervalJoinExpressionAnnotation getIntervalJoinAnnotation(AbstractFunctionCallExpression fexp) {
        Iterator<IExpressionAnnotation> annotationIter = fexp.getAnnotations().values().iterator();
        while (annotationIter.hasNext()) {
            IExpressionAnnotation annotation = annotationIter.next();
            if (annotation instanceof IntervalJoinExpressionAnnotation) {
                return (IntervalJoinExpressionAnnotation) annotation;
            }
        }
        return null;
    }

    private static void setSortMergeIntervalJoinOp(AbstractBinaryJoinOperator op, IOptimizationContext context) {
        op.setPhysicalOperator(new SortMergeIntervalJoinPOperator(op.getJoinKind(), JoinPartitioningType.BROADCAST, context
                .getPhysicalOptimizationConfig().getMaxRecordsPerFrame()));
    }

    private static void setNLJoinOp(AbstractBinaryJoinOperator op, IOptimizationContext context) {
        op.setPhysicalOperator(new NLJoinPOperator(op.getJoinKind(), JoinPartitioningType.BROADCAST, context
                .getPhysicalOptimizationConfig().getMaxRecordsPerFrame()));
    }

    private static void setHashJoinOp(AbstractBinaryJoinOperator op, JoinPartitioningType partitioningType,
            List<LogicalVariable> sideLeft, List<LogicalVariable> sideRight, IOptimizationContext context)
            throws AlgebricksException {
        op.setPhysicalOperator(new HybridHashJoinPOperator(op.getJoinKind(), partitioningType, sideLeft, sideRight,
                context.getPhysicalOptimizationConfig().getMaxFramesHybridHash(), context
                        .getPhysicalOptimizationConfig().getMaxFramesLeftInputHybridHash(), context
                        .getPhysicalOptimizationConfig().getMaxRecordsPerFrame(), context
                        .getPhysicalOptimizationConfig().getFudgeFactor()));
    }

    private static boolean isIntervalJoinCondition(ILogicalExpression e, Collection<LogicalVariable> inLeftAll,
            Collection<LogicalVariable> inRightAll, Collection<LogicalVariable> outLeftFields,
            Collection<LogicalVariable> outRightFields) {
        switch (e.getExpressionTag()) {
            case FUNCTION_CALL: {
                AbstractFunctionCallExpression fexp = (AbstractFunctionCallExpression) e;
                FunctionIdentifier fi = fexp.getFunctionIdentifier();
                if (!fi.equals(AsterixBuiltinFunctions.INTERVAL_OVERLAPS)) {
                    return false;
                }
                ILogicalExpression opLeft = fexp.getArguments().get(0).getValue();
                ILogicalExpression opRight = fexp.getArguments().get(1).getValue();
                if (opLeft.getExpressionTag() != LogicalExpressionTag.VARIABLE
                        || opRight.getExpressionTag() != LogicalExpressionTag.VARIABLE) {
                    return false;
                }
                LogicalVariable var1 = ((VariableReferenceExpression) opLeft).getVariableReference();
                if (inLeftAll.contains(var1) && !outLeftFields.contains(var1)) {
                    outLeftFields.add(var1);
                } else if (inRightAll.contains(var1) && !outRightFields.contains(var1)) {
                    outRightFields.add(var1);
                } else {
                    return false;
                }
                LogicalVariable var2 = ((VariableReferenceExpression) opRight).getVariableReference();
                if (inLeftAll.contains(var2) && !outLeftFields.contains(var2)) {
                    outLeftFields.add(var2);
                } else if (inRightAll.contains(var2) && !outRightFields.contains(var2)) {
                    outRightFields.add(var2);
                } else {
                    return false;
                }
                return true;
            }
            default: {
                return false;
            }
        }
    }
}

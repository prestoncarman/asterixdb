/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.optimizer.rules.util;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

import org.apache.asterix.algebra.operators.physical.interval.SortMergeIntervalJoinPOperator;
import org.apache.asterix.common.annotations.IntervalJoinExpressionAnnotation;
import org.apache.asterix.dataflow.data.nontagged.comparators.allenrelations.AllenRelationsBinaryComparatorFactoryProvider;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.IExpressionAnnotation;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.AbstractJoinPOperator.JoinPartitioningType;
import org.apache.hyracks.algebricks.data.IBinaryComparatorFactoryProvider;
import org.apache.hyracks.dataflow.common.data.partition.range.IRangeMap;

public class JoinUtils {

    private static final Logger LOGGER = Logger.getLogger(JoinUtils.class.getName());

    public static void setJoinAlgorithmAndExchangeAlgo(AbstractBinaryJoinOperator op, IOptimizationContext context)
            throws AlgebricksException {
        List<LogicalVariable> sideLeft = new LinkedList<LogicalVariable>();
        List<LogicalVariable> sideRight = new LinkedList<LogicalVariable>();
        List<LogicalVariable> varsLeft = op.getInputs().get(0).getValue().getSchema();
        List<LogicalVariable> varsRight = op.getInputs().get(1).getValue().getSchema();
        ILogicalExpression conditionLE = op.getCondition().getValue();
        if (conditionLE.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return;
        }
        AbstractFunctionCallExpression fexp = (AbstractFunctionCallExpression) conditionLE;
        if (isIntervalJoinCondition(fexp, varsLeft, varsRight, sideLeft, sideRight)) {
            IntervalJoinExpressionAnnotation ijea = getIntervalJoinAnnotation(fexp);
            if (ijea == null) {
                // Use default join method.
                return;
            }
            if (ijea.isMergeJoin()) {
                // Sort Merge.
                LOGGER.fine("Interval Join - Merge");
                setSortMergeIntervalJoinOp(op, sideLeft, sideRight, ijea.getRangeMap(), context);
            } else if (ijea.isIopJoin()) {
                // Overlapping Interval Partition.
                LOGGER.fine("Interval Join - IOP");
            } else if (ijea.isSpatialJoin()) {
                // Spatial Partition.
                LOGGER.fine("Interval Join - Spatial Partitioning");
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

    private static void setSortMergeIntervalJoinOp(AbstractBinaryJoinOperator op, List<LogicalVariable> sideLeft,
            List<LogicalVariable> sideRight, IRangeMap rangeMap, IOptimizationContext context) {
        IBinaryComparatorFactoryProvider bcfp = (IBinaryComparatorFactoryProvider) AllenRelationsBinaryComparatorFactoryProvider.INSTANCE;
        op.setPhysicalOperator(new SortMergeIntervalJoinPOperator(op.getJoinKind(), JoinPartitioningType.BROADCAST,
                context.getPhysicalOptimizationConfig().getMaxRecordsPerFrame(), sideLeft, sideRight, bcfp, rangeMap));
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

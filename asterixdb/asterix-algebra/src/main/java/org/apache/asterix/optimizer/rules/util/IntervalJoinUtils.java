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

import static org.apache.hyracks.algebricks.core.algebra.properties.IPartitioningProperty.PartitioningType.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.asterix.algebra.operators.physical.IntervalMergeJoinPOperator;
import org.apache.asterix.common.annotations.RangeAnnotation;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.lang.common.util.FunctionUtil;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.runtime.operators.joins.interval.utils.AfterIntervalJoinUtilFactory;
import org.apache.asterix.runtime.operators.joins.interval.utils.BeforeIntervalJoinUtilFactory;
import org.apache.asterix.runtime.operators.joins.interval.utils.CoveredByIntervalJoinUtilFactory;
import org.apache.asterix.runtime.operators.joins.interval.utils.CoversIntervalJoinUtilFactory;
import org.apache.asterix.runtime.operators.joins.interval.utils.IIntervalJoinUtilFactory;
import org.apache.asterix.runtime.operators.joins.interval.utils.OverlappedByIntervalJoinUtilFactory;
import org.apache.asterix.runtime.operators.joins.interval.utils.OverlappingIntervalJoinUtilFactory;
import org.apache.asterix.runtime.operators.joins.interval.utils.OverlapsIntervalJoinUtilFactory;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.common.utils.Quadruple;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.IPhysicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractLogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ExchangeOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ForwardOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.OrderOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ReplicateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnionAllOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.AbstractJoinPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.AggregatePOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.AssignPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.BroadcastExchangePOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.OneToOneExchangePOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.PartialBroadcastRangeFollowingExchangePOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.PartialBroadcastRangeIntersectExchangePOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.RangePartitionExchangePOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.ReplicatePOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.SortForwardPOperator;
import org.apache.hyracks.algebricks.core.algebra.properties.IPartitioningProperty.PartitioningType;
import org.apache.hyracks.algebricks.core.algebra.properties.IntervalColumn;
import org.apache.hyracks.algebricks.core.algebra.properties.OrderColumn;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
import org.apache.hyracks.algebricks.rewriter.util.PhysicalOptimizationsUtil;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.api.exceptions.Warning;
import org.apache.hyracks.dataflow.common.data.partition.range.RangeMap;

public class IntervalJoinUtils {

    private static final Map<FunctionIdentifier, FunctionIdentifier> INTERVAL_JOIN_CONDITIONS = new HashMap<>();

    static {
        INTERVAL_JOIN_CONDITIONS.put(BuiltinFunctions.INTERVAL_AFTER, BuiltinFunctions.INTERVAL_BEFORE);
        INTERVAL_JOIN_CONDITIONS.put(BuiltinFunctions.INTERVAL_BEFORE, BuiltinFunctions.INTERVAL_AFTER);
        INTERVAL_JOIN_CONDITIONS.put(BuiltinFunctions.INTERVAL_COVERED_BY, BuiltinFunctions.INTERVAL_COVERS);
        INTERVAL_JOIN_CONDITIONS.put(BuiltinFunctions.INTERVAL_COVERS, BuiltinFunctions.INTERVAL_COVERED_BY);
        INTERVAL_JOIN_CONDITIONS.put(BuiltinFunctions.INTERVAL_OVERLAPPED_BY, BuiltinFunctions.INTERVAL_OVERLAPS);
        INTERVAL_JOIN_CONDITIONS.put(BuiltinFunctions.INTERVAL_OVERLAPPING, BuiltinFunctions.INTERVAL_OVERLAPPING);
        INTERVAL_JOIN_CONDITIONS.put(BuiltinFunctions.INTERVAL_OVERLAPS, BuiltinFunctions.INTERVAL_OVERLAPPED_BY);
    }

    protected static boolean tryIntervalJoinAssignment(AbstractBinaryJoinOperator op, IOptimizationContext context,
            ILogicalExpression joinCondition, int left, int right) throws AlgebricksException {
        List<LogicalVariable> sideLeft = new ArrayList<>(1);
        List<LogicalVariable> sideRight = new ArrayList<>(1);
        List<LogicalVariable> varsLeft = op.getInputs().get(left).getValue().getSchema();
        List<LogicalVariable> varsRight = op.getInputs().get(right).getValue().getSchema();

        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) joinCondition;
        FunctionIdentifier fi = IntervalJoinUtils.isIntervalJoinCondition(funcExpr, varsLeft, varsRight, sideLeft,
                sideRight, left, right);
        if (fi == null) {
            return false;
        }

        // Existing workflow for interval merge join
        RangeAnnotation rangeAnnotation = IntervalJoinUtils.findRangeAnnotation(funcExpr);
        if (rangeAnnotation == null) {
            return false;
        }

        //Check RangeMap type
        RangeMap rangeMap = rangeAnnotation.getRangeMap();
        if (rangeMap.getTag(0, 0) != ATypeTag.DATETIME.serialize() && rangeMap.getTag(0, 0) != ATypeTag.DATE.serialize()
                && rangeMap.getTag(0, 0) != ATypeTag.TIME.serialize()) {
            IWarningCollector warningCollector = context.getWarningCollector();
            if (warningCollector.shouldWarn()) {
                warningCollector.warn(Warning.of(op.getSourceLocation(),
                        org.apache.hyracks.api.exceptions.ErrorCode.INAPPLICABLE_HINT,
                        "Date, DateTime, and Time are only range hints types supported for interval joins"));
            }
            return false;
        }

        IntervalPartitions intervalPartitions = IntervalJoinUtils.createIntervalPartitions(op, fi, sideLeft, sideRight,
                context, left, right, rangeMap, null);
        IntervalJoinUtils.setSortMergeIntervalJoinOp(op, fi, sideLeft, sideRight, context, intervalPartitions);
        return true;
    }

    protected static RangeAnnotation findRangeAnnotation(AbstractFunctionCallExpression fexp) {
        return fexp.getAnnotation(RangeAnnotation.class);
    }

    protected static FunctionIdentifier isIntervalJoinCondition(ILogicalExpression e,
            Collection<LogicalVariable> inLeftAll, Collection<LogicalVariable> inRightAll,
            Collection<LogicalVariable> outLeftFields, Collection<LogicalVariable> outRightFields, int left,
            int right) {
        FunctionIdentifier fiReturn;
        boolean switchArguments = false;
        if (e.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return null;
        }
        AbstractFunctionCallExpression fexp = (AbstractFunctionCallExpression) e;
        FunctionIdentifier fi = fexp.getFunctionIdentifier();
        if (isIntervalFunction(fi)) {
            fiReturn = fi;
        } else {
            return null;
        }
        ILogicalExpression opLeft = fexp.getArguments().get(left).getValue();
        ILogicalExpression opRight = fexp.getArguments().get(right).getValue();
        if (opLeft.getExpressionTag() != LogicalExpressionTag.VARIABLE
                || opRight.getExpressionTag() != LogicalExpressionTag.VARIABLE) {
            return null;
        }
        LogicalVariable var1 = ((VariableReferenceExpression) opLeft).getVariableReference();
        if (inLeftAll.contains(var1) && !outLeftFields.contains(var1)) {
            outLeftFields.add(var1);
        } else if (inRightAll.contains(var1) && !outRightFields.contains(var1)) {
            outRightFields.add(var1);
            fiReturn = getInverseIntervalFunction(fi);
            switchArguments = true;
        } else {
            return null;
        }
        LogicalVariable var2 = ((VariableReferenceExpression) opRight).getVariableReference();
        if (inLeftAll.contains(var2) && !outLeftFields.contains(var2) && switchArguments) {
            outLeftFields.add(var2);
        } else if (inRightAll.contains(var2) && !outRightFields.contains(var2) && !switchArguments) {
            outRightFields.add(var2);
        } else {
            return null;
        }
        return fiReturn;
    }

    protected static void updateJoinPlan(AbstractBinaryJoinOperator op, IOptimizationContext context,
            AbstractFunctionCallExpression fexp, int left, int right, RangeAnnotation rangeAnnotation)
            throws AlgebricksException {

        List<LogicalVariable> varsLeft = op.getInputs().get(left).getValue().getSchema();
        List<LogicalVariable> varsRight = op.getInputs().get(right).getValue().getSchema();
        List<LogicalVariable> sideLeft = new ArrayList<>(1);
        List<LogicalVariable> sideRight = new ArrayList<>(1);

        boolean switchArguments = false;
        if (fexp.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return;
        }

        FunctionIdentifier fi = fexp.getFunctionIdentifier();
        if (!isIntervalFunction(fi)) {
            return;
        }

        ILogicalExpression opLeft = fexp.getArguments().get(left).getValue();
        ILogicalExpression opRight = fexp.getArguments().get(right).getValue();

        if (opLeft.getExpressionTag() != LogicalExpressionTag.VARIABLE
                || opRight.getExpressionTag() != LogicalExpressionTag.VARIABLE) {
            return;
        }

        LogicalVariable var1 = ((VariableReferenceExpression) opLeft).getVariableReference();
        if (varsLeft.contains(var1)) {
            sideLeft.add(var1);
        } else if (varsRight.contains(var1)) {
            sideRight.add(var1);
            fi = getInverseIntervalFunction(fi);
            switchArguments = true;
        } else {
            return;
        }

        LogicalVariable var2 = ((VariableReferenceExpression) opRight).getVariableReference();
        if (varsLeft.contains(var2) && !sideLeft.contains(var2) && switchArguments) {
            sideLeft.add(var2);
        } else if (varsRight.contains(var2) && !sideRight.contains(var2) && !switchArguments) {
            sideRight.add(var2);
        } else {
            return;
        }

        if (fi == null) {
            return;
        }

        if (rangeAnnotation == null) {

            buildSortMergeIntervalPlanWithDynamicHint(op, context, fi, sideLeft, sideRight, left, right);
        } else {
            buildSortMergeIntervalPlanWithStaticHint(op, context, fi, sideLeft, sideRight, left, right,
                    rangeAnnotation);
        }
    }

    private static boolean isIntervalFunction(FunctionIdentifier fi) {
        return INTERVAL_JOIN_CONDITIONS.containsKey(fi);
    }

    /**
     * Certain Relations not yet supported as seen below. Will default to regular join.
     * Inserts partition sort key.
     */
    protected static IntervalPartitions createIntervalPartitions(AbstractBinaryJoinOperator op, FunctionIdentifier fi,
            List<LogicalVariable> sideLeft, List<LogicalVariable> sideRight, IOptimizationContext context, int left,
            int right, RangeMap rangeMap, String rangeMapKey) throws AlgebricksException {

        List<LogicalVariable> leftPartitionVar = new ArrayList<>(2);
        leftPartitionVar.add(context.newVar());
        leftPartitionVar.add(context.newVar());
        List<LogicalVariable> rightPartitionVar = new ArrayList<>(2);
        rightPartitionVar.add(context.newVar());
        rightPartitionVar.add(context.newVar());

        insertPartitionSortKey(op, left, leftPartitionVar, sideLeft.get(0), context);
        insertPartitionSortKey(op, right, rightPartitionVar, sideRight.get(0), context);

        List<IntervalColumn> leftIC = Collections.singletonList(new IntervalColumn(leftPartitionVar.get(0),
                leftPartitionVar.get(1), OrderOperator.IOrder.OrderKind.ASC));
        List<IntervalColumn> rightIC = Collections.singletonList(new IntervalColumn(rightPartitionVar.get(0),
                rightPartitionVar.get(1), OrderOperator.IOrder.OrderKind.ASC));

        //Set Partitioning Types
        PartitioningType leftPartitioningType = ORDERED_PARTITIONED;
        PartitioningType rightPartitioningType = ORDERED_PARTITIONED;
        if (fi.equals(BuiltinFunctions.INTERVAL_OVERLAPPED_BY)) {
            rightPartitioningType = PARTIAL_BROADCAST_ORDERED_INTERSECT;
        } else if (fi.equals(BuiltinFunctions.INTERVAL_OVERLAPS)) {
            leftPartitioningType = PARTIAL_BROADCAST_ORDERED_INTERSECT;
        } else if (fi.equals(BuiltinFunctions.INTERVAL_OVERLAPPING)) {
            leftPartitioningType = PARTIAL_BROADCAST_ORDERED_INTERSECT;
            rightPartitioningType = PARTIAL_BROADCAST_ORDERED_INTERSECT;
        } else if (fi.equals(BuiltinFunctions.INTERVAL_COVERS)) {
            leftPartitioningType = PARTIAL_BROADCAST_ORDERED_INTERSECT;
        } else if (fi.equals(BuiltinFunctions.INTERVAL_COVERED_BY)) {
            rightPartitioningType = PARTIAL_BROADCAST_ORDERED_INTERSECT;
        } else if (fi.equals(BuiltinFunctions.INTERVAL_BEFORE)) {
            leftPartitioningType = PARTIAL_BROADCAST_ORDERED_FOLLOWING;
        } else if (fi.equals(BuiltinFunctions.INTERVAL_AFTER)) {
            rightPartitioningType = PARTIAL_BROADCAST_ORDERED_FOLLOWING;
        } else {
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, fi.getName());
        }
        return new IntervalPartitions(leftIC, rightIC, leftPartitioningType, rightPartitioningType, rangeMap,
                rangeMapKey);
    }

    protected static Triple<List<LogicalVariable>, List<LogicalVariable>, IntervalPartitions> createIntervalPartitionsDynamic(
            FunctionIdentifier fi, IOptimizationContext context, RangeMap rangeMap, String rangeMapKey)
            throws AlgebricksException {

        List<LogicalVariable> leftPartitionVar = new ArrayList<>(2);
        leftPartitionVar.add(context.newVar());
        leftPartitionVar.add(context.newVar());
        List<LogicalVariable> rightPartitionVar = new ArrayList<>(2);
        rightPartitionVar.add(context.newVar());
        rightPartitionVar.add(context.newVar());

        List<IntervalColumn> leftIC = Collections.singletonList(new IntervalColumn(leftPartitionVar.get(0),
                leftPartitionVar.get(1), OrderOperator.IOrder.OrderKind.ASC));
        List<IntervalColumn> rightIC = Collections.singletonList(new IntervalColumn(rightPartitionVar.get(0),
                rightPartitionVar.get(1), OrderOperator.IOrder.OrderKind.ASC));

        //Set Partitioning Types
        PartitioningType leftPartitioningType = ORDERED_PARTITIONED;
        PartitioningType rightPartitioningType = ORDERED_PARTITIONED;
        if (fi.equals(BuiltinFunctions.INTERVAL_OVERLAPPED_BY)) {
            rightPartitioningType = PARTIAL_BROADCAST_ORDERED_INTERSECT;
        } else if (fi.equals(BuiltinFunctions.INTERVAL_OVERLAPS)) {
            leftPartitioningType = PARTIAL_BROADCAST_ORDERED_INTERSECT;
        } else if (fi.equals(BuiltinFunctions.INTERVAL_OVERLAPPING)) {
            leftPartitioningType = PARTIAL_BROADCAST_ORDERED_INTERSECT;
            rightPartitioningType = PARTIAL_BROADCAST_ORDERED_INTERSECT;
        } else if (fi.equals(BuiltinFunctions.INTERVAL_COVERS)) {
            leftPartitioningType = PARTIAL_BROADCAST_ORDERED_INTERSECT;
        } else if (fi.equals(BuiltinFunctions.INTERVAL_COVERED_BY)) {
            rightPartitioningType = PARTIAL_BROADCAST_ORDERED_INTERSECT;
        } else if (fi.equals(BuiltinFunctions.INTERVAL_BEFORE)) {
            leftPartitioningType = PARTIAL_BROADCAST_ORDERED_FOLLOWING;
        } else if (fi.equals(BuiltinFunctions.INTERVAL_AFTER)) {
            rightPartitioningType = PARTIAL_BROADCAST_ORDERED_FOLLOWING;
        } else {
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, fi.getName());
        }
        return new Triple<>(leftPartitionVar, rightPartitionVar, new IntervalPartitions(leftIC, rightIC,
                leftPartitioningType, rightPartitioningType, rangeMap, rangeMapKey));
    }

    private static void insertPartitionSortKey(AbstractBinaryJoinOperator op, int branch,
            List<LogicalVariable> partitionVars, LogicalVariable intervalVar, IOptimizationContext context)
            throws AlgebricksException {
        List<Mutable<ILogicalExpression>> assignExps = new ArrayList<>();
        // Start partition
        VariableReferenceExpression intervalVarRef1 = new VariableReferenceExpression(intervalVar);
        intervalVarRef1.setSourceLocation(op.getSourceLocation());
        IFunctionInfo startFi = FunctionUtil.getFunctionInfo(BuiltinFunctions.ACCESSOR_TEMPORAL_INTERVAL_START);
        ScalarFunctionCallExpression startPartitionExp =
                new ScalarFunctionCallExpression(startFi, new MutableObject<>(intervalVarRef1));
        startPartitionExp.setSourceLocation(op.getSourceLocation());
        assignExps.add(new MutableObject<>(startPartitionExp));
        // End partition
        VariableReferenceExpression intervalVarRef2 = new VariableReferenceExpression(intervalVar);
        intervalVarRef2.setSourceLocation(op.getSourceLocation());
        IFunctionInfo endFi = FunctionUtil.getFunctionInfo(BuiltinFunctions.ACCESSOR_TEMPORAL_INTERVAL_END);
        ScalarFunctionCallExpression endPartitionExp =
                new ScalarFunctionCallExpression(endFi, new MutableObject<>(intervalVarRef2));
        endPartitionExp.setSourceLocation(op.getSourceLocation());
        assignExps.add(new MutableObject<>(endPartitionExp));

        AssignOperator ao = new AssignOperator(partitionVars, assignExps);
        ao.setSourceLocation(op.getSourceLocation());
        ao.setExecutionMode(AbstractLogicalOperator.ExecutionMode.PARTITIONED);
        AssignPOperator apo = new AssignPOperator();
        ao.setPhysicalOperator(apo);
        Mutable<ILogicalOperator> aoRef = new MutableObject<>(ao);
        ao.getInputs().add(op.getInputs().get(branch));
        op.getInputs().set(branch, aoRef);

        context.computeAndSetTypeEnvironmentForOperator(ao);
        ao.recomputeSchema();
    }

    /**
     * Certain Relations not yet supported as seen below. Will default to regular join.
     */
    private static IIntervalJoinUtilFactory createIntervalJoinCheckerFactory(FunctionIdentifier fi, RangeMap rangeMap,
            String rangeMapKey) throws CompilationException {
        IIntervalJoinUtilFactory mjcf;
        if (fi.equals(BuiltinFunctions.INTERVAL_OVERLAPPED_BY)) {
            mjcf = new OverlappedByIntervalJoinUtilFactory();
        } else if (fi.equals(BuiltinFunctions.INTERVAL_OVERLAPS)) {
            mjcf = new OverlapsIntervalJoinUtilFactory();
        } else if (fi.equals(BuiltinFunctions.INTERVAL_COVERS)) {
            mjcf = new CoversIntervalJoinUtilFactory();
        } else if (fi.equals(BuiltinFunctions.INTERVAL_COVERED_BY)) {
            mjcf = new CoveredByIntervalJoinUtilFactory();
        } else if (fi.equals(BuiltinFunctions.INTERVAL_BEFORE)) {
            mjcf = new BeforeIntervalJoinUtilFactory();
        } else if (fi.equals(BuiltinFunctions.INTERVAL_AFTER)) {
            mjcf = new AfterIntervalJoinUtilFactory();
        } else if (fi.equals(BuiltinFunctions.INTERVAL_OVERLAPPING)) {
            mjcf = new OverlappingIntervalJoinUtilFactory(rangeMap, rangeMapKey);
        } else {
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, fi.getName());
        }
        return mjcf;
    }

    protected static void setSortMergeIntervalJoinOp(AbstractBinaryJoinOperator op, FunctionIdentifier fi,
            List<LogicalVariable> sideLeft, List<LogicalVariable> sideRight, IOptimizationContext context,
            IntervalPartitions intervalPartitions) throws CompilationException {
        IIntervalJoinUtilFactory mjcf = createIntervalJoinCheckerFactory(fi, intervalPartitions.getRangeMap(), null);
        op.setPhysicalOperator(new IntervalMergeJoinPOperator(op.getJoinKind(),
                AbstractJoinPOperator.JoinPartitioningType.BROADCAST, sideLeft, sideRight,
                context.getPhysicalOptimizationConfig().getMaxFramesForJoin(), mjcf, intervalPartitions));
    }

    private static FunctionIdentifier getInverseIntervalFunction(FunctionIdentifier fi) {
        return INTERVAL_JOIN_CONDITIONS.get(fi);
    }

    protected static void buildSortMergeIntervalPlanWithStaticHint(AbstractBinaryJoinOperator op,
            IOptimizationContext context, FunctionIdentifier fi, List<LogicalVariable> sideLeft,
            List<LogicalVariable> sideRight, int left, int right, RangeAnnotation rangeAnnotation)
            throws AlgebricksException {
        //Check RangeMap type
        RangeMap rangeMap = rangeAnnotation.getRangeMap();
        if (rangeMap.getTag(0, 0) != ATypeTag.DATETIME.serialize() && rangeMap.getTag(0, 0) != ATypeTag.DATE.serialize()
                && rangeMap.getTag(0, 0) != ATypeTag.TIME.serialize()) {
            IWarningCollector warningCollector = context.getWarningCollector();
            if (warningCollector.shouldWarn()) {
                warningCollector.warn(Warning.of(op.getSourceLocation(),
                        org.apache.hyracks.api.exceptions.ErrorCode.INAPPLICABLE_HINT,
                        "Date, DateTime, and Time are only range hints types supported for interval joins"));
            }
            return;
        }

        IntervalPartitions intervalPartitions =
                createIntervalPartitions(op, fi, sideLeft, sideRight, context, left, right, rangeMap, null);
        setSortMergeIntervalJoinOp(op, fi, sideLeft, sideRight, context, intervalPartitions);
    }

    protected static void buildSortMergeIntervalPlanWithDynamicHint(AbstractBinaryJoinOperator op,
            IOptimizationContext context, FunctionIdentifier fi, List<LogicalVariable> sideLeft,
            List<LogicalVariable> sideRight, int left, int right) throws AlgebricksException {

        Mutable<ILogicalOperator> leftInputOp = op.getInputs().get(left);
        Mutable<ILogicalOperator> rightInputOp = op.getInputs().get(right);

        // Add a dynamic workflow to compute Range of the left branch
        // Add a dynamic workflow to compute Range of the right branch
        Triple<MutableObject<ILogicalOperator>, List<LogicalVariable>, MutableObject<ILogicalOperator>> leftRangeCalculator =
                createDynamicRangeCalculator(op, context, leftInputOp, sideLeft);
        MutableObject<ILogicalOperator> leftGlobalRangeAggregateOperator = leftRangeCalculator.first;
        List<LogicalVariable> leftGlobalAggResultVars = leftRangeCalculator.second;
        MutableObject<ILogicalOperator> inputToLeftForwardOperator = leftRangeCalculator.third;
        LogicalVariable leftRangeGlobalAggregateVar = leftGlobalAggResultVars.get(0);

        // Add a dynamic workflow to compute Range of the right branch
        Triple<MutableObject<ILogicalOperator>, List<LogicalVariable>, MutableObject<ILogicalOperator>> rightRangeCalculator =
                createDynamicRangeCalculator(op, context, rightInputOp, sideRight);
        MutableObject<ILogicalOperator> rightGlobalRangeAggregateOperator = rightRangeCalculator.first;
        List<LogicalVariable> rightGlobalAggResultVars = rightRangeCalculator.second;
        MutableObject<ILogicalOperator> inputToRightForwardOperator = rightRangeCalculator.third;
        LogicalVariable rightRangeGlobalAggregateVar = rightGlobalAggResultVars.get(0);

        // Union the results of left and right aggregators to get the union Range of both left and right input
        LogicalVariable unionRangeVar = context.newVar();
        Triple<LogicalVariable, LogicalVariable, LogicalVariable> unionVarMap =
                new Triple<>(leftRangeGlobalAggregateVar, rightRangeGlobalAggregateVar, unionRangeVar);
        List<Triple<LogicalVariable, LogicalVariable, LogicalVariable>> unionVarMaps = new ArrayList<>();
        unionVarMaps.add(unionVarMap);
        UnionAllOperator unionAllOperator = new UnionAllOperator(unionVarMaps);
        unionAllOperator.setSourceLocation(op.getSourceLocation());
        unionAllOperator.getInputs().add(new MutableObject<>(leftGlobalRangeAggregateOperator.getValue()));
        unionAllOperator.getInputs().add(new MutableObject<>(rightGlobalRangeAggregateOperator.getValue()));
        OperatorManipulationUtil.setOperatorMode(unionAllOperator);
        unionAllOperator.recomputeSchema();
        context.computeAndSetTypeEnvironmentForOperator(unionAllOperator);
        MutableObject<ILogicalOperator> unionAllOperatorRef = new MutableObject<>(unionAllOperator);

        Triple<List<LogicalVariable>, List<LogicalVariable>, IntervalPartitions> intervalPartitionsInfo =
                createIntervalPartitionsDynamic(fi, context, null, null);
        List<LogicalVariable> leftPartitionVar = intervalPartitionsInfo.first;
        List<LogicalVariable> rightPartitionVar = intervalPartitionsInfo.second;
        IntervalPartitions intervalPartitions = intervalPartitionsInfo.third;

        // Compute the union Range of the left and the right Range
        Pair<MutableObject<ILogicalOperator>, List<LogicalVariable>> globalAggregateOperator =
                createGlobalRangeMapAggregateOperator(op, context, unionRangeVar, unionAllOperatorRef,
                        intervalPartitions.getLeftStartColumn());
        MutableObject<ILogicalOperator> globalAgg = globalAggregateOperator.getFirst();
        LogicalVariable rangeMapAggregateOperatorVar = globalAggregateOperator.getSecond().get(0);

        // Replicate Range Map To Forward Operators
        Quadruple<MutableObject<ILogicalOperator>, MutableObject<ILogicalOperator>, MutableObject<ILogicalOperator>, MutableObject<ILogicalOperator>> rangeMapOutputs =
                replicateRangeMap(op, context, globalAgg);
        MutableObject<ILogicalOperator> exchRangeMapToForwardLeftRef = rangeMapOutputs.getFirst();
        MutableObject<ILogicalOperator> exchRangeMapToForwardRightRef = rangeMapOutputs.getSecond();
        MutableObject<ILogicalOperator> exchRangeMapToAfterLeftForwardRef = rangeMapOutputs.getThird();
        MutableObject<ILogicalOperator> exchRangeMapToAfterRightForwardRef = rangeMapOutputs.getFourth();

        // Create the left forward operator
        String leftRangeMapKey = UUID.randomUUID().toString();
        ForwardOperator leftForward = createForward(leftRangeMapKey, rangeMapAggregateOperatorVar,
                inputToLeftForwardOperator, exchRangeMapToForwardLeftRef, context, op.getSourceLocation());
        MutableObject<ILogicalOperator> leftForwardRef = new MutableObject<>(leftForward);
        leftInputOp.setValue(leftForwardRef.getValue());

        // Create the right forward operator
        String rightRangeMapKey = UUID.randomUUID().toString();
        ForwardOperator rightForward = createForward(rightRangeMapKey, rangeMapAggregateOperatorVar,
                inputToRightForwardOperator, exchRangeMapToForwardRightRef, context, op.getSourceLocation());
        MutableObject<ILogicalOperator> rightForwardRef = new MutableObject<>(rightForward);
        rightInputOp.setValue(rightForwardRef.getValue());

        insertPartitionSortKey(op, left, leftPartitionVar, sideLeft.get(0), context);
        insertPartitionSortKey(op, right, rightPartitionVar, sideRight.get(0), context);

        leftInputOp = op.getInputs().get(left);
        rightInputOp = op.getInputs().get(right);

        // Set Left Partitioning Physical Operators
        ExchangeOperator exchangeLeft = setPartitioningExchangeOperator(leftInputOp, context, fi,
                intervalPartitions.getLeftPartitioningType(), intervalPartitions.getLeftStartColumn(),
                intervalPartitions.getLeftIntervalColumn(), leftRangeMapKey);
        MutableObject<ILogicalOperator> inputToLeftAfterForwardOperator = new MutableObject<>(exchangeLeft);

        // Set Right Partitioning Physical Operators
        ExchangeOperator exchangeRight = setPartitioningExchangeOperator(rightInputOp, context, fi,
                intervalPartitions.getRightPartitioningType(), intervalPartitions.getRightStartColumn(),
                intervalPartitions.getRightIntervalColumn(), rightRangeMapKey);
        MutableObject<ILogicalOperator> inputToRightAfterForwardOperator = new MutableObject<>(exchangeRight);

        // Create the left after forward operator
        String leftAfterRangeMapKey = UUID.randomUUID().toString();
        ForwardOperator leftAfterForward = createForward(leftAfterRangeMapKey, rangeMapAggregateOperatorVar,
                inputToLeftAfterForwardOperator, exchRangeMapToAfterLeftForwardRef, context, op.getSourceLocation());
        MutableObject<ILogicalOperator> leftAfterForwardRef = new MutableObject<>(leftAfterForward);
        leftInputOp.setValue(leftAfterForwardRef.getValue());

        // Create the right after forward operator
        String rightAfterRangeMapKey = UUID.randomUUID().toString();
        ForwardOperator rightAfterForward = createForward(rightAfterRangeMapKey, rangeMapAggregateOperatorVar,
                inputToRightAfterForwardOperator, exchRangeMapToAfterRightForwardRef, context, op.getSourceLocation());
        MutableObject<ILogicalOperator> rightAfterForwardRef = new MutableObject<>(rightAfterForward);
        rightInputOp.setValue(rightAfterForwardRef.getValue());

        intervalPartitions.setRangeMapKey(leftAfterRangeMapKey);

        IIntervalJoinUtilFactory mjcf = createIntervalJoinCheckerFactory(fi, null, intervalPartitions.getRangeMapKey());
        op.setPhysicalOperator(new IntervalMergeJoinPOperator(op.getJoinKind(),
                AbstractJoinPOperator.JoinPartitioningType.BROADCAST, sideLeft, sideRight,
                context.getPhysicalOptimizationConfig().getMaxFramesForJoin(), mjcf, intervalPartitions));
    }

    private static Quadruple<MutableObject<ILogicalOperator>, MutableObject<ILogicalOperator>, MutableObject<ILogicalOperator>, MutableObject<ILogicalOperator>> replicateRangeMap(
            AbstractBinaryJoinOperator op, IOptimizationContext context, Mutable<ILogicalOperator> inputOp)
            throws AlgebricksException {
        // Replicate the union Range Map to left and right forward operator
        ReplicateOperator unionRangeMapReplicateOperator =
                createReplicateOperator(inputOp, context, op.getSourceLocation(), 4);
        ExchangeOperator exchRangeMapToForwardLeft = createBroadcastExchangeOp(unionRangeMapReplicateOperator, context);
        MutableObject<ILogicalOperator> exchRangeMapToForwardLeftRef = new MutableObject<>(exchRangeMapToForwardLeft);
        ExchangeOperator exchRangeMapToForwardRight =
                createBroadcastExchangeOp(unionRangeMapReplicateOperator, context);
        MutableObject<ILogicalOperator> exchRangeMapToForwardRightRef = new MutableObject<>(exchRangeMapToForwardRight);
        ExchangeOperator exchRangeMapToAfterLeftForward =
                createBroadcastExchangeOp(unionRangeMapReplicateOperator, context);
        MutableObject<ILogicalOperator> exchRangeMapToAfterLeftForwardRef =
                new MutableObject<>(exchRangeMapToAfterLeftForward);
        ExchangeOperator exchRangeMapToAfterRightForward =
                createBroadcastExchangeOp(unionRangeMapReplicateOperator, context);
        MutableObject<ILogicalOperator> exchRangeMapToAfterRightForwardRef =
                new MutableObject<>(exchRangeMapToAfterRightForward);

        return new Quadruple<>(exchRangeMapToForwardLeftRef, exchRangeMapToForwardRightRef,
                exchRangeMapToAfterLeftForwardRef, exchRangeMapToAfterRightForwardRef);
    }

    private static ExchangeOperator setPartitioningExchangeOperator(Mutable<ILogicalOperator> inputOp,
            IOptimizationContext context, FunctionIdentifier fi, PartitioningType partitioningType,
            List<OrderColumn> startColumn, List<IntervalColumn> intervalColumn, String rangeMapKey)
            throws AlgebricksException {
        // Set up left connectors
        IPhysicalOperator pop;
        switch (partitioningType) {
            case ORDERED_PARTITIONED: {
                pop = new RangePartitionExchangePOperator(startColumn, context.getComputationNodeDomain(), rangeMapKey);
                break;
            }

            case PARTIAL_BROADCAST_ORDERED_FOLLOWING: {
                pop = new PartialBroadcastRangeFollowingExchangePOperator(startColumn,
                        context.getComputationNodeDomain(), rangeMapKey);
                break;
            }
            case PARTIAL_BROADCAST_ORDERED_INTERSECT: {
                pop = new PartialBroadcastRangeIntersectExchangePOperator(intervalColumn,
                        context.getComputationNodeDomain(), rangeMapKey);
                break;
            }
            default:
                throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, fi.getName());
        }

        ExchangeOperator exchg = new ExchangeOperator();
        exchg.setPhysicalOperator(pop);
        ILogicalOperator oldOp = inputOp.getValue();
        inputOp.setValue(exchg);
        exchg.getInputs().add(new MutableObject<>(oldOp));
        exchg.recomputeSchema();
        exchg.computeDeliveredPhysicalProperties(context);
        context.computeAndSetTypeEnvironmentForOperator(exchg);
        PhysicalOptimizationsUtil.computeFDsAndEquivalenceClasses(exchg, context);
        exchg.setExecutionMode(AbstractLogicalOperator.ExecutionMode.PARTITIONED);
        OperatorPropertiesUtil.computeSchemaAndPropertiesRecIfNull(exchg, context);
        context.computeAndSetTypeEnvironmentForOperator(exchg);
        return exchg;
    }

    private static Triple<MutableObject<ILogicalOperator>, List<LogicalVariable>, MutableObject<ILogicalOperator>> createDynamicRangeCalculator(
            AbstractBinaryJoinOperator op, IOptimizationContext context, Mutable<ILogicalOperator> inputOp,
            List<LogicalVariable> inputVars) throws AlgebricksException {
        // Add ReplicationOperator for the input branch
        SourceLocation sourceLocation = op.getSourceLocation();
        ReplicateOperator replicateOperator = createReplicateOperator(inputOp, context, sourceLocation, 2);

        // Create one to one exchange operators for the replicator of the input branch
        ExchangeOperator exchToForward = createOneToOneExchangeOp(replicateOperator, context);
        MutableObject<ILogicalOperator> exchToForwardRef = new MutableObject<>(exchToForward);

        ExchangeOperator exchToLocalAgg = createOneToOneExchangeOp(replicateOperator, context);
        MutableObject<ILogicalOperator> exchToLocalAggRef = new MutableObject<>(exchToLocalAgg);

        // Materialize the data to be able to re-read the data again
        replicateOperator.getOutputMaterializationFlags()[0] = true;

        Pair<MutableObject<ILogicalOperator>, List<LogicalVariable>> createLocalAndGlobalAggResult =
                createLocalAndGlobalAggregateOperators(op, context, inputVars, exchToLocalAggRef);
        return new Triple<>(createLocalAndGlobalAggResult.first, createLocalAndGlobalAggResult.second,
                exchToForwardRef);
    }

    private static Pair<MutableObject<ILogicalOperator>, List<LogicalVariable>> createLocalAndGlobalAggregateOperators(
            AbstractBinaryJoinOperator op, IOptimizationContext context, List<LogicalVariable> inputVars,
            MutableObject<ILogicalOperator> exchToLocalAggRef) throws AlgebricksException {
        AbstractLogicalExpression inputVarRef;
        List<Mutable<ILogicalExpression>> fields = new ArrayList<>(1);
        for (LogicalVariable inputVar : inputVars) {
            inputVarRef = new VariableReferenceExpression(inputVar, op.getSourceLocation());
            fields.add(new MutableObject<>(inputVarRef));
        }

        // Create local aggregate operator
        IFunctionInfo localAggFunc =
                context.getMetadataProvider().lookupFunction(BuiltinFunctions.LOCAL_INTERVAL_RANGE);
        AggregateFunctionCallExpression localAggExpr = new AggregateFunctionCallExpression(localAggFunc, false, fields);
        localAggExpr.setSourceLocation(op.getSourceLocation());
        localAggExpr.setOpaqueParameters(new Object[] {});
        List<LogicalVariable> localAggResultVars = new ArrayList<>(1);
        List<Mutable<ILogicalExpression>> localAggFuncs = new ArrayList<>(1);
        LogicalVariable localOutVariable = context.newVar();
        localAggResultVars.add(localOutVariable);
        localAggFuncs.add(new MutableObject<>(localAggExpr));
        AggregateOperator localAggOperator = createAggregate(localAggResultVars, false, localAggFuncs,
                exchToLocalAggRef, context, op.getSourceLocation());
        MutableObject<ILogicalOperator> localAgg = new MutableObject<>(localAggOperator);

        // Output of local aggregate operator is the input of global aggregate operator
        return createGlobalAggregateOperator(op, context, localOutVariable, localAgg);
    }

    private static Pair<MutableObject<ILogicalOperator>, List<LogicalVariable>> createGlobalAggregateOperator(
            AbstractBinaryJoinOperator op, IOptimizationContext context, LogicalVariable inputVar,
            MutableObject<ILogicalOperator> inputOperator) throws AlgebricksException {
        List<Mutable<ILogicalExpression>> globalAggFuncArgs = new ArrayList<>(1);
        AbstractLogicalExpression inputVarRef = new VariableReferenceExpression(inputVar, op.getSourceLocation());
        globalAggFuncArgs.add(new MutableObject<>(inputVarRef));
        IFunctionInfo globalAggFunc =
                context.getMetadataProvider().lookupFunction(BuiltinFunctions.GLOBAL_INTERVAL_RANGE);
        AggregateFunctionCallExpression globalAggExpr =
                new AggregateFunctionCallExpression(globalAggFunc, true, globalAggFuncArgs);
        globalAggExpr.setStepOneAggregate(globalAggFunc);
        globalAggExpr.setStepTwoAggregate(globalAggFunc);
        globalAggExpr.setSourceLocation(op.getSourceLocation());
        globalAggExpr.setOpaqueParameters(new Object[] {});
        List<LogicalVariable> globalAggResultVars = new ArrayList<>(1);
        globalAggResultVars.add(context.newVar());
        List<Mutable<ILogicalExpression>> globalAggFuncs = new ArrayList<>(1);
        globalAggFuncs.add(new MutableObject<>(globalAggExpr));
        AggregateOperator globalAggOperator = createAggregate(globalAggResultVars, true, globalAggFuncs, inputOperator,
                context, op.getSourceLocation());
        globalAggOperator.recomputeSchema();
        context.computeAndSetTypeEnvironmentForOperator(globalAggOperator);
        MutableObject<ILogicalOperator> globalAgg = new MutableObject<>(globalAggOperator);
        return new Pair<>(globalAgg, globalAggResultVars);
    }

    private static Pair<MutableObject<ILogicalOperator>, List<LogicalVariable>> createGlobalRangeMapAggregateOperator(
            AbstractBinaryJoinOperator op, IOptimizationContext context, LogicalVariable inputVar,
            MutableObject<ILogicalOperator> inputOperator, List<OrderColumn> partitionFields)
            throws AlgebricksException {
        List<Mutable<ILogicalExpression>> globalAggFuncArgs = new ArrayList<>(1);
        AbstractLogicalExpression inputVarRef = new VariableReferenceExpression(inputVar, op.getSourceLocation());
        globalAggFuncArgs.add(new MutableObject<>(inputVarRef));
        IFunctionInfo globalAggFunc =
                context.getMetadataProvider().lookupFunction(BuiltinFunctions.GLOBAL_INTERVAL_RANGE_MAP);
        AggregateFunctionCallExpression globalAggExpr =
                new AggregateFunctionCallExpression(globalAggFunc, true, globalAggFuncArgs);
        globalAggExpr.setStepOneAggregate(globalAggFunc);
        globalAggExpr.setStepTwoAggregate(globalAggFunc);
        globalAggExpr.setSourceLocation(op.getSourceLocation());

        int numPartitions = context.getComputationNodeDomain().cardinality();

        boolean[] ascendingFlags = new boolean[partitionFields.size()];
        int i = 0;
        for (OrderColumn field : partitionFields) {
            ascendingFlags[i] = field.getOrder() == OrderOperator.IOrder.OrderKind.ASC;
            i++;
        }

        globalAggExpr.setOpaqueParameters(new Object[] { numPartitions, ascendingFlags });
        List<LogicalVariable> globalAggResultVars = new ArrayList<>(1);
        globalAggResultVars.add(context.newVar());
        List<Mutable<ILogicalExpression>> globalAggFuncs = new ArrayList<>(1);
        globalAggFuncs.add(new MutableObject<>(globalAggExpr));
        AggregateOperator globalAggOperator = createAggregate(globalAggResultVars, true, globalAggFuncs, inputOperator,
                context, op.getSourceLocation());
        globalAggOperator.recomputeSchema();
        context.computeAndSetTypeEnvironmentForOperator(globalAggOperator);
        MutableObject<ILogicalOperator> globalAgg = new MutableObject<>(globalAggOperator);
        return new Pair<>(globalAgg, globalAggResultVars);
    }

    private static AggregateOperator createAggregate(List<LogicalVariable> resultVariables, boolean isGlobal,
            List<Mutable<ILogicalExpression>> expressions, MutableObject<ILogicalOperator> inputOperator,
            IOptimizationContext context, SourceLocation sourceLocation) throws AlgebricksException {
        AggregateOperator aggregateOperator = new AggregateOperator(resultVariables, expressions);
        aggregateOperator.setPhysicalOperator(new AggregatePOperator());
        aggregateOperator.setSourceLocation(sourceLocation);
        aggregateOperator.getInputs().add(inputOperator);
        aggregateOperator.setGlobal(isGlobal);
        if (!isGlobal) {
            aggregateOperator.setExecutionMode(AbstractLogicalOperator.ExecutionMode.LOCAL);
        } else {
            aggregateOperator.setExecutionMode(AbstractLogicalOperator.ExecutionMode.UNPARTITIONED);
        }
        aggregateOperator.recomputeSchema();
        context.computeAndSetTypeEnvironmentForOperator(aggregateOperator);
        return aggregateOperator;
    }

    private static ReplicateOperator createReplicateOperator(Mutable<ILogicalOperator> inputOperator,
            IOptimizationContext context, SourceLocation sourceLocation, int outputArity) throws AlgebricksException {
        ReplicateOperator replicateOperator = new ReplicateOperator(outputArity);
        replicateOperator.setPhysicalOperator(new ReplicatePOperator());
        replicateOperator.setSourceLocation(sourceLocation);
        replicateOperator.getInputs().add(new MutableObject<>(inputOperator.getValue()));
        OperatorManipulationUtil.setOperatorMode(replicateOperator);
        replicateOperator.recomputeSchema();
        context.computeAndSetTypeEnvironmentForOperator(replicateOperator);
        return replicateOperator;
    }

    private static ForwardOperator createForward(String aggResultKey, LogicalVariable aggResultVariable,
            MutableObject<ILogicalOperator> exchangeOpFromReplicate, MutableObject<ILogicalOperator> globalAggInput,
            IOptimizationContext context, SourceLocation sourceLoc) throws AlgebricksException {
        AbstractLogicalExpression rangeMapExpression = new VariableReferenceExpression(aggResultVariable, sourceLoc);
        ForwardOperator forwardOperator = new ForwardOperator(aggResultKey, new MutableObject<>(rangeMapExpression));
        forwardOperator.setSourceLocation(sourceLoc);
        forwardOperator.setPhysicalOperator(new SortForwardPOperator());
        forwardOperator.getInputs().add(exchangeOpFromReplicate);
        forwardOperator.getInputs().add(globalAggInput);
        OperatorManipulationUtil.setOperatorMode(forwardOperator);
        forwardOperator.recomputeSchema();
        context.computeAndSetTypeEnvironmentForOperator(forwardOperator);
        return forwardOperator;
    }

    private static ExchangeOperator createBroadcastExchangeOp(ReplicateOperator replicateOperator,
            IOptimizationContext context) throws AlgebricksException {
        ExchangeOperator exchangeOperator = new ExchangeOperator();
        exchangeOperator.setPhysicalOperator(new BroadcastExchangePOperator(context.getComputationNodeDomain()));
        replicateOperator.getOutputs().add(new MutableObject<>(exchangeOperator));
        exchangeOperator.getInputs().add(new MutableObject<>(replicateOperator));
        exchangeOperator.setExecutionMode(AbstractLogicalOperator.ExecutionMode.PARTITIONED);
        exchangeOperator.setSchema(replicateOperator.getSchema());
        context.computeAndSetTypeEnvironmentForOperator(exchangeOperator);
        return exchangeOperator;
    }

    private static ExchangeOperator createOneToOneExchangeOp(ReplicateOperator replicateOperator,
            IOptimizationContext context) throws AlgebricksException {
        ExchangeOperator exchangeOperator = new ExchangeOperator();
        exchangeOperator.setPhysicalOperator(new OneToOneExchangePOperator());
        replicateOperator.getOutputs().add(new MutableObject<>(exchangeOperator));
        exchangeOperator.getInputs().add(new MutableObject<>(replicateOperator));
        exchangeOperator.setExecutionMode(AbstractLogicalOperator.ExecutionMode.PARTITIONED);
        exchangeOperator.setSchema(replicateOperator.getSchema());
        context.computeAndSetTypeEnvironmentForOperator(exchangeOperator);
        return exchangeOperator;
    }
}

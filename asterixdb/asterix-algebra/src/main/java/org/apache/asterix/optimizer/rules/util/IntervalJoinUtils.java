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

import static org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator.ExecutionMode.UNPARTITIONED;
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
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.AMutableInterval;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.pointables.nonvisitor.AIntervalPointable;
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
import org.apache.hyracks.algebricks.common.utils.ListSet;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.common.utils.Quadruple;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.IPhysicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.*;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.*;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.*;
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

        AbstractFunctionCallExpression fexp = (AbstractFunctionCallExpression) joinCondition;

        RangeAnnotation rangeAnnotation = IntervalJoinUtils.findRangeAnnotation(fexp);
        return IntervalJoinUtils.updateJoinPlan(op, context, fexp, left, right, rangeAnnotation);
    }

    protected static RangeAnnotation findRangeAnnotation(AbstractFunctionCallExpression fexp) {
        return fexp.getAnnotation(RangeAnnotation.class);
    }

    protected static void setSortMergeIntervalJoinOp(AbstractBinaryJoinOperator op, FunctionIdentifier fi,
            List<LogicalVariable> sideLeft, List<LogicalVariable> sideRight, IOptimizationContext context,
            IntervalPartitions intervalPartitions) throws CompilationException {
        IIntervalJoinUtilFactory mjcf = createIntervalJoinCheckerFactory(fi, intervalPartitions.getRangeMap(), null);
        op.setPhysicalOperator(new IntervalMergeJoinPOperator(op.getJoinKind(),
                AbstractJoinPOperator.JoinPartitioningType.BROADCAST, sideLeft, sideRight,
                context.getPhysicalOptimizationConfig().getMaxFramesForJoin(), mjcf, intervalPartitions));
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

    protected static boolean updateJoinPlan(AbstractBinaryJoinOperator op, IOptimizationContext context,
            AbstractFunctionCallExpression intervalJoinFuncExpr, int left, int right, RangeAnnotation rangeAnnotation)
            throws AlgebricksException {

        List<LogicalVariable> varsLeft = op.getInputs().get(left).getValue().getSchema();
        List<LogicalVariable> varsRight = op.getInputs().get(right).getValue().getSchema();
        List<LogicalVariable> keysLeftBranch = new ArrayList<>(1);
        List<LogicalVariable> keysRightBranch = new ArrayList<>(1);

        boolean switchArguments = false;
        if (intervalJoinFuncExpr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }


        FunctionIdentifier fi = intervalJoinFuncExpr.getFunctionIdentifier();
        if (!isIntervalFunction(fi)) {
            return false;
        }
        // Check for num of arguments

        ILogicalExpression intervalJoinLeftArg = intervalJoinFuncExpr.getArguments().get(left).getValue();
        ILogicalExpression intervalJoinRightArg = intervalJoinFuncExpr.getArguments().get(right).getValue();

        if (intervalJoinLeftArg.getExpressionTag() != LogicalExpressionTag.VARIABLE
                || intervalJoinRightArg.getExpressionTag() != LogicalExpressionTag.VARIABLE) {
            return false;
        }

        // Extract left and right variable of the predicate
        LogicalVariable intervalJoinVar0 = ((VariableReferenceExpression) intervalJoinLeftArg).getVariableReference();
        LogicalVariable intervalJoinVar1 = ((VariableReferenceExpression) intervalJoinRightArg).getVariableReference();

        if (varsLeft.contains(intervalJoinVar0)) {
            keysLeftBranch.add(intervalJoinVar0);
        } else if (varsRight.contains(intervalJoinVar0)) {
            keysRightBranch.add(intervalJoinVar0);
            fi = getInverseIntervalFunction(fi);
            switchArguments = true;
        } else {
            return false;
        }

        if (varsLeft.contains(intervalJoinVar1) && !keysLeftBranch.contains(intervalJoinVar1) && switchArguments) {
            keysLeftBranch.add(intervalJoinVar1);
        } else if (varsRight.contains(intervalJoinVar1) && !keysRightBranch.contains(intervalJoinVar1) && !switchArguments) {
            keysRightBranch.add(intervalJoinVar1);
        } else {
            return false;
        }

        if (fi == null) {
            return false;
        }

        if (rangeAnnotation == null) {
            buildSortMergeIntervalPlanWithDynamicHint(op, context, fi, keysLeftBranch, keysRightBranch, left, right);
        } else {
            buildSortMergeIntervalPlanWithStaticHint(op, context, fi, keysLeftBranch, keysRightBranch, left, right,
                    rangeAnnotation);
        }
        return true;
    }
    //change left and right names leftinputkey

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
//        Triple<MutableObject<ILogicalOperator>, List<LogicalVariable>, MutableObject<ILogicalOperator>> leftRangeCalculator =
//                createDynamicRangeCalculator(op, context, leftInputOp, sideLeft);
//        MutableObject<ILogicalOperator> leftGlobalRangeAggregateOperator = leftRangeCalculator.first;
//        List<LogicalVariable> leftGlobalAggResultVars = leftRangeCalculator.second;
//        MutableObject<ILogicalOperator> inputToLeftForwardOperator = leftRangeCalculator.third;
//        LogicalVariable leftRangeGlobalAggregateVar = leftGlobalAggResultVars.get(0);
//
//        // Add a dynamic workflow to compute Range of the right branch
//        Triple<MutableObject<ILogicalOperator>, List<LogicalVariable>, MutableObject<ILogicalOperator>> rightRangeCalculator =
//                createDynamicRangeCalculator(op, context, rightInputOp, sideRight);
//        MutableObject<ILogicalOperator> rightGlobalRangeAggregateOperator = rightRangeCalculator.first;
//        List<LogicalVariable> rightGlobalAggResultVars = rightRangeCalculator.second;
//        MutableObject<ILogicalOperator> inputToRightForwardOperator = rightRangeCalculator.third;
//        LogicalVariable rightRangeGlobalAggregateVar = rightGlobalAggResultVars.get(0);
//
//        // Join the left and right range overlap
//        Mutable<ILogicalExpression> trueCondition =
//                new MutableObject<>(new ConstantExpression(new AsterixConstantValue(ABoolean.TRUE)));
//        InnerJoinOperator unionIntervalJoinOp = new InnerJoinOperator(trueCondition, leftGlobalRangeAggregateOperator, rightGlobalRangeAggregateOperator);
//        unionIntervalJoinOp.setSourceLocation(op.getSourceLocation());
//        unionIntervalJoinOp.setPhysicalOperator(new NestedLoopJoinPOperator(AbstractBinaryJoinOperator.JoinKind.INNER,
//                AbstractJoinPOperator.JoinPartitioningType.BROADCAST));
//        MutableObject<ILogicalOperator> unionIntervalJoinOpRef = new MutableObject<>(unionIntervalJoinOp);
//        unionIntervalJoinOp.recomputeSchema();
//        context.computeAndSetTypeEnvironmentForOperator(unionIntervalJoinOp);


        // Compute the range map of left and right
//        List<Mutable<ILogicalExpression>> getIntersectionFuncInputExprs2 = new ArrayList<>();
//        getIntersectionFuncInputExprs2.add(new MutableObject<>(new VariableReferenceExpression(leftRangeGlobalAggregateVar)));
//        getIntersectionFuncInputExprs2.add(new MutableObject<>(new VariableReferenceExpression(rightRangeGlobalAggregateVar)));
//        getIntersectionFuncInputExprs2.add(new MutableObject<>(new ConstantExpression(new AsterixConstantValue(new AInt32(context.getComputationNodeDomain().cardinality())))));
//        ScalarFunctionCallExpression getIntersectionFuncExpr = new ScalarFunctionCallExpression(
//                BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.INTERVAL_RANGE_MAP),
//                getIntersectionFuncInputExprs2);
//        getIntersectionFuncExpr.setSourceLocation(op.getSourceLocation());
//
//        Mutable<ILogicalExpression> intersectionIntervalExpr2 = new MutableObject<>(getIntersectionFuncExpr);
//        LogicalVariable rangemapVariable2 = context.newVar();
//        AbstractLogicalOperator intersectionIntervalAssignOperator2 =
//                new AssignOperator(rangemapVariable2, intersectionIntervalExpr2);
//        intersectionIntervalAssignOperator2.setSourceLocation(op.getSourceLocation());
//        intersectionIntervalAssignOperator2.setExecutionMode(op.getExecutionMode());
//        intersectionIntervalAssignOperator2.setPhysicalOperator(new AssignPOperator());
//        intersectionIntervalAssignOperator2.getInputs().add(new MutableObject<>(unionIntervalJoinOpRef.getValue()));
//        context.computeAndSetTypeEnvironmentForOperator(intersectionIntervalAssignOperator2);
//        intersectionIntervalAssignOperator2.recomputeSchema();
//        MutableObject<ILogicalOperator> intersectionIntervalAssignOperatorRef2 =
//                new MutableObject<>(intersectionIntervalAssignOperator2);


        // Replicate the range map to left and right forward operator and another forward for the join
//        ReplicateOperator intersectionMAPReplicateOperator =
//                createReplicateOperator(intersectionIntervalAssignOperatorRef, context, op.getSourceLocation(), 3);

//        ExchangeOperator exchMAPToJoinOpLeft =
//                createBroadcastExchangeOp(intersectionMAPReplicateOperator, context, op.getSourceLocation());
//        MutableObject<ILogicalOperator> exchMAPToJoinOpLeftRef = new MutableObject<>(exchMAPToJoinOpLeft);
//
//        ExchangeOperator exchMAPToAfterLeftForward =
//                createBroadcastExchangeOp(intersectionMAPReplicateOperator, context, op.getSourceLocation());
//        MutableObject<ILogicalOperator> exchRangeMapToAfterLeftForwardRef = new MutableObject<>(exchMAPToAfterLeftForward);
//
//        // Replicate to the right branch
//        ExchangeOperator exchMAPToJoinOpRight =
//                createBroadcastExchangeOp(intersectionMAPReplicateOperator, context, op.getSourceLocation());
//        MutableObject<ILogicalOperator> exchMAPToJoinOpRightRef = new MutableObject<>(exchMAPToJoinOpRight);


        EmptyTupleSourceOperator ets = new EmptyTupleSourceOperator();
        ets.setExecutionMode(UNPARTITIONED);
        ets.setPhysicalOperator(new EmptyTupleSourcePOperator());

        List<Mutable<ILogicalExpression>> getIntersectionFuncInputExprs = new ArrayList<>();
        getIntersectionFuncInputExprs.add(new MutableObject<>(new ConstantExpression(new AsterixConstantValue(new AMutableInterval(0, 100, (byte) 17)))));
        getIntersectionFuncInputExprs.add(new MutableObject<>(new ConstantExpression(new AsterixConstantValue(new AMutableInterval(0, 100, (byte) 17)))));
        getIntersectionFuncInputExprs.add(new MutableObject<>(new ConstantExpression(new AsterixConstantValue(new AInt32(context.getComputationNodeDomain().cardinality())))));
        ScalarFunctionCallExpression getIntersectionFuncExpr = new ScalarFunctionCallExpression(
                BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.INTERVAL_RANGE_MAP),
                getIntersectionFuncInputExprs);
        getIntersectionFuncExpr.setSourceLocation(op.getSourceLocation());

        Mutable<ILogicalExpression> intersectionIntervalExpr = new MutableObject<>(getIntersectionFuncExpr);
        LogicalVariable rangemapVariable = context.newVar();
        AbstractLogicalOperator intersectionIntervalAssignOperator =
                new AssignOperator(rangemapVariable, intersectionIntervalExpr);
        intersectionIntervalAssignOperator.setSourceLocation(op.getSourceLocation());
        intersectionIntervalAssignOperator.setExecutionMode(op.getExecutionMode());
        intersectionIntervalAssignOperator.setPhysicalOperator(new AssignPOperator());
        intersectionIntervalAssignOperator.getInputs().add(new MutableObject<>(ets));
        context.computeAndSetTypeEnvironmentForOperator(intersectionIntervalAssignOperator);
        ArrayList<LogicalVariable> rangeMapSchema = new ArrayList<>();
        rangeMapSchema.add(rangemapVariable);
        intersectionIntervalAssignOperator.setSchema(rangeMapSchema);
        MutableObject<ILogicalOperator> intersectionIntervalAssignOperatorRef =
                new MutableObject<>(intersectionIntervalAssignOperator);

        EmptyTupleSourceOperator ets2 = new EmptyTupleSourceOperator();
        ets2.setExecutionMode(UNPARTITIONED);
        ets2.setPhysicalOperator(new EmptyTupleSourcePOperator());

        List<Mutable<ILogicalExpression>> getIntersectionFuncInputExprs2 = new ArrayList<>();
        getIntersectionFuncInputExprs2.add(new MutableObject<>(new ConstantExpression(new AsterixConstantValue(new AMutableInterval(0, 100, (byte) 17)))));
        getIntersectionFuncInputExprs2.add(new MutableObject<>(new ConstantExpression(new AsterixConstantValue(new AMutableInterval(0, 100, (byte) 17)))));
        getIntersectionFuncInputExprs2.add(new MutableObject<>(new ConstantExpression(new AsterixConstantValue(new AInt32(context.getComputationNodeDomain().cardinality())))));
        ScalarFunctionCallExpression getIntersectionFuncExpr2 = new ScalarFunctionCallExpression(
                BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.INTERVAL_RANGE_MAP),
                getIntersectionFuncInputExprs2);
        getIntersectionFuncExpr2.setSourceLocation(op.getSourceLocation());

        Mutable<ILogicalExpression> intersectionIntervalExpr2 = new MutableObject<>(getIntersectionFuncExpr2);
        LogicalVariable rangemapVariable2 = context.newVar();
        AbstractLogicalOperator intersectionIntervalAssignOperator2 =
                new AssignOperator(rangemapVariable2, intersectionIntervalExpr2);
        intersectionIntervalAssignOperator2.setSourceLocation(op.getSourceLocation());
        intersectionIntervalAssignOperator2.setExecutionMode(op.getExecutionMode());
        intersectionIntervalAssignOperator2.setPhysicalOperator(new AssignPOperator());
        intersectionIntervalAssignOperator2.getInputs().add(new MutableObject<>(ets2));
        context.computeAndSetTypeEnvironmentForOperator(intersectionIntervalAssignOperator2);
        ArrayList<LogicalVariable> rangeMapSchema2 = new ArrayList<>();
        rangeMapSchema2.add(rangemapVariable2);
        intersectionIntervalAssignOperator2.setSchema(rangeMapSchema2);
        MutableObject<ILogicalOperator> intersectionIntervalAssignOperatorRef2 =
                new MutableObject<>(intersectionIntervalAssignOperator2);


        String rangeMapKey = UUID.randomUUID().toString();
        // Create the left forward operator
        ForwardOperator leftForward = createForward(rangeMapKey, rangemapVariable,
                new MutableObject<>(leftInputOp.getValue()), intersectionIntervalAssignOperatorRef, context, op.getSourceLocation());
        MutableObject<ILogicalOperator> leftForwardRef = new MutableObject<>(leftForward);
        leftInputOp.setValue(leftForwardRef.getValue());

        // Create the right forward operator
        ForwardOperator rightForward = createForward(rangeMapKey, rangemapVariable2,
                new MutableObject<>(rightInputOp.getValue()), intersectionIntervalAssignOperatorRef2, context, op.getSourceLocation());
        MutableObject<ILogicalOperator> rightForwardRef = new MutableObject<>(rightForward);
        rightInputOp.setValue(rightForwardRef.getValue());

        IntervalPartitions intervalPartitions =
                createIntervalPartitions(op, fi, sideLeft, sideRight, context, left, right, null, rangeMapKey);

        Mutable<ILogicalOperator> leftPartitionSortOp = op.getInputs().get(left);
        Mutable<ILogicalOperator> rightPartitionSortOp = op.getInputs().get(right);

        // Set Left Partitioning Physical Operators
        ExchangeOperator exchangeLeft = setPartitioningExchangeOperator(leftPartitionSortOp, context, fi,
                intervalPartitions.getLeftPartitioningType(), intervalPartitions.getLeftStartColumn(),
                intervalPartitions.getLeftIntervalColumn(), rangeMapKey);
        MutableObject<ILogicalOperator> exchangeLeftRef =  new MutableObject<>(exchangeLeft);

        // Set Right Partitioning Physical Operators
        setPartitioningExchangeOperator(rightPartitionSortOp, context, fi,
                intervalPartitions.getRightPartitioningType(), intervalPartitions.getRightStartColumn(),
                intervalPartitions.getRightIntervalColumn(), rangeMapKey);

        EmptyTupleSourceOperator ets3 = new EmptyTupleSourceOperator();
        ets3.setExecutionMode(UNPARTITIONED);
        ets3.setPhysicalOperator(new EmptyTupleSourcePOperator());

        List<Mutable<ILogicalExpression>> getIntersectionFuncInputExprs3 = new ArrayList<>();
        getIntersectionFuncInputExprs3.add(new MutableObject<>(new ConstantExpression(new AsterixConstantValue(new AMutableInterval(0, 100, (byte) 17)))));
        getIntersectionFuncInputExprs3.add(new MutableObject<>(new ConstantExpression(new AsterixConstantValue(new AMutableInterval(0, 100, (byte) 17)))));
        getIntersectionFuncInputExprs3.add(new MutableObject<>(new ConstantExpression(new AsterixConstantValue(new AInt32(context.getComputationNodeDomain().cardinality())))));
        ScalarFunctionCallExpression getIntersectionFuncExpr3 = new ScalarFunctionCallExpression(
                BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.INTERVAL_RANGE_MAP),
                getIntersectionFuncInputExprs3);
        getIntersectionFuncExpr3.setSourceLocation(op.getSourceLocation());

        Mutable<ILogicalExpression> intersectionIntervalExpr3 = new MutableObject<>(getIntersectionFuncExpr3);
        LogicalVariable rangemapVariable3 = context.newVar();
        AbstractLogicalOperator intersectionIntervalAssignOperator3 =
                new AssignOperator(rangemapVariable3, intersectionIntervalExpr3);
        intersectionIntervalAssignOperator3.setSourceLocation(op.getSourceLocation());
        intersectionIntervalAssignOperator3.setExecutionMode(op.getExecutionMode());
        intersectionIntervalAssignOperator3.setPhysicalOperator(new AssignPOperator());
        intersectionIntervalAssignOperator3.getInputs().add(new MutableObject<>(ets3));
        context.computeAndSetTypeEnvironmentForOperator(intersectionIntervalAssignOperator3);
        ArrayList<LogicalVariable> rangeMapSchema3 = new ArrayList<>();
        rangeMapSchema3.add(rangemapVariable3);
        intersectionIntervalAssignOperator3.setSchema(rangeMapSchema3);
        MutableObject<ILogicalOperator> intersectionIntervalAssignOperatorRef3 =
                new MutableObject<>(intersectionIntervalAssignOperator3);

         //Create the left after forward operator
        ForwardOperator leftAfterForward = createForward(rangeMapKey, rangemapVariable3,
                exchangeLeftRef, intersectionIntervalAssignOperatorRef3, context, op.getSourceLocation());
        MutableObject<ILogicalOperator> leftAfterForwardRef = new MutableObject<>(leftAfterForward);
        leftPartitionSortOp.setValue(leftAfterForwardRef.getValue());


        IIntervalJoinUtilFactory mjcf = createIntervalJoinCheckerFactory(fi, null, intervalPartitions.getRangeMapKey());
        op.setPhysicalOperator(new IntervalMergeJoinPOperator(op.getJoinKind(),
                AbstractJoinPOperator.JoinPartitioningType.BROADCAST, sideLeft, sideRight,
                context.getPhysicalOptimizationConfig().getMaxFramesForJoin(), mjcf, intervalPartitions));
    }

    private static boolean isIntervalFunction(FunctionIdentifier fi) {
        return INTERVAL_JOIN_CONDITIONS.containsKey(fi);
    }

    private static FunctionIdentifier getInverseIntervalFunction(FunctionIdentifier fi) {
        return INTERVAL_JOIN_CONDITIONS.get(fi);
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

    private static Quadruple<MutableObject<ILogicalOperator>, MutableObject<ILogicalOperator>, MutableObject<ILogicalOperator>, MutableObject<ILogicalOperator>> replicateRangeMap(
            AbstractBinaryJoinOperator op, IOptimizationContext context, Mutable<ILogicalOperator> inputOp)
            throws AlgebricksException {
        // Replicate the union Range Map to left and right forward operator
        ReplicateOperator unionRangeMapReplicateOperator =
                createReplicateOperator(inputOp, context, op.getSourceLocation(), 4);
        ExchangeOperator exchRangeMapToForwardLeft = createBroadcastExchangeOp(unionRangeMapReplicateOperator, context, op.getSourceLocation());
        MutableObject<ILogicalOperator> exchRangeMapToForwardLeftRef = new MutableObject<>(exchRangeMapToForwardLeft);
        ExchangeOperator exchRangeMapToForwardRight =
                createBroadcastExchangeOp(unionRangeMapReplicateOperator, context,op.getSourceLocation());
        MutableObject<ILogicalOperator> exchRangeMapToForwardRightRef = new MutableObject<>(exchRangeMapToForwardRight);
        ExchangeOperator exchRangeMapToAfterLeftForward =
                createBroadcastExchangeOp(unionRangeMapReplicateOperator, context, op.getSourceLocation());
        MutableObject<ILogicalOperator> exchRangeMapToAfterLeftForwardRef =
                new MutableObject<>(exchRangeMapToAfterLeftForward);
        ExchangeOperator exchRangeMapToAfterRightForward =
                createBroadcastExchangeOp(unionRangeMapReplicateOperator, context, op.getSourceLocation());
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
                context.getMetadataProvider().lookupFunction(BuiltinFunctions.LOCAL_UNION_INTERVAL_RANGE);
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
                context.getMetadataProvider().lookupFunction(BuiltinFunctions.GLOBAL_UNION_INTERVAL_RANGE);
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

    //TODO do not and eventually remove
    private static Pair<MutableObject<ILogicalOperator>, List<LogicalVariable>> createGlobalRangeMapAggregateOperator(
            AbstractBinaryJoinOperator op, IOptimizationContext context, LogicalVariable inputVar,
            MutableObject<ILogicalOperator> inputOperator, List<OrderColumn> partitionFields)
            throws AlgebricksException {
        List<Mutable<ILogicalExpression>> globalAggFuncArgs = new ArrayList<>(1);
        AbstractLogicalExpression inputVarRef = new VariableReferenceExpression(inputVar, op.getSourceLocation());
        globalAggFuncArgs.add(new MutableObject<>(inputVarRef));
        IFunctionInfo globalAggFunc =
                context.getMetadataProvider().lookupFunction(BuiltinFunctions.INTERVAL_RANGE_MAP);
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

    private static Pair<LogicalVariable, Mutable<ILogicalOperator>> createAssignProjectOperator(
            AbstractBinaryJoinOperator op, LogicalVariable inputVar, ReplicateOperator replicateOperator,
            MutableObject<ILogicalOperator> exchMBRToForwardRef, IOptimizationContext context)
            throws AlgebricksException {
        LogicalVariable newFinalMbrVar = context.newVar();
        List<LogicalVariable> finalMBRLiveVars = new ArrayList<>();
        finalMBRLiveVars.add(newFinalMbrVar);
        ListSet<LogicalVariable> finalMBRLiveVarsSet = new ListSet<>();
        finalMBRLiveVarsSet.add(newFinalMbrVar);

        Mutable<ILogicalExpression> finalMBRExpr = new MutableObject<>(new VariableReferenceExpression(inputVar));
        AbstractLogicalOperator assignOperator = new AssignOperator(newFinalMbrVar, finalMBRExpr);
        assignOperator.setSourceLocation(op.getSourceLocation());
        assignOperator.setExecutionMode(replicateOperator.getExecutionMode());
        assignOperator.setPhysicalOperator(new AssignPOperator());
        AbstractLogicalOperator projectOperator = new ProjectOperator(finalMBRLiveVars);
        projectOperator.setSourceLocation(op.getSourceLocation());
        projectOperator.setPhysicalOperator(new StreamProjectPOperator());
        projectOperator.setExecutionMode(replicateOperator.getExecutionMode());
        assignOperator.getInputs().add(exchMBRToForwardRef);
        projectOperator.getInputs().add(new MutableObject<ILogicalOperator>(assignOperator));

        context.computeAndSetTypeEnvironmentForOperator(assignOperator);
        assignOperator.recomputeSchema();
        context.computeAndSetTypeEnvironmentForOperator(projectOperator);
        projectOperator.recomputeSchema();
        Mutable<ILogicalOperator> projectOperatorRef = new MutableObject<>(projectOperator);

        return new Pair<>(newFinalMbrVar, projectOperatorRef);
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

    private static ForwardOperator createForward(String rangeMapKey, LogicalVariable rangeMapVariable,
            MutableObject<ILogicalOperator> inputDataOperator, MutableObject<ILogicalOperator> inputRangeMapOperator,
            IOptimizationContext context, SourceLocation sourceLoc) throws AlgebricksException {
        AbstractLogicalExpression rangeMapExpression = new VariableReferenceExpression(rangeMapVariable, sourceLoc);
        ForwardOperator forwardOperator = new ForwardOperator(rangeMapKey, new MutableObject<>(rangeMapExpression));
        forwardOperator.setSourceLocation(sourceLoc);
        forwardOperator.setPhysicalOperator(new SortForwardPOperator());
        forwardOperator.getInputs().add(inputDataOperator);
        forwardOperator.getInputs().add(inputRangeMapOperator);
        OperatorManipulationUtil.setOperatorMode(forwardOperator);
        forwardOperator.recomputeSchema();
        context.computeAndSetTypeEnvironmentForOperator(forwardOperator);
        return forwardOperator;
    }

    private static ExchangeOperator createBroadcastExchangeOp(ReplicateOperator replicateOperator,
            IOptimizationContext context, SourceLocation sourceLocation) throws AlgebricksException {
        ExchangeOperator exchangeOperator = new ExchangeOperator();
        exchangeOperator.setSourceLocation(sourceLocation);
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

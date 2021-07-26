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

import java.util.ArrayList;
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
import org.apache.hyracks.algebricks.common.utils.ListSet;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
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
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ProjectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ReplicateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnionAllOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.AbstractJoinPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.AggregatePOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.AssignPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.BroadcastExchangePOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.OneToOneExchangePOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.ReplicatePOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.SortForwardPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.StreamProjectPOperator;
import org.apache.hyracks.algebricks.core.algebra.properties.IPartitioningProperty.PartitioningType;
import org.apache.hyracks.algebricks.core.algebra.properties.IntervalColumn;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;
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

    protected static RangeAnnotation findRangeAnnotation(AbstractFunctionCallExpression fexp) {
        return fexp.getAnnotation(RangeAnnotation.class);
    }

    protected static void tryIntervalJoinAssignment(AbstractBinaryJoinOperator op, IOptimizationContext context,
            ILogicalExpression joinCondition, int left, int right) throws AlgebricksException {

        AbstractFunctionCallExpression fexp = (AbstractFunctionCallExpression) joinCondition;

        RangeAnnotation rangeAnnotation = IntervalJoinUtils.findRangeAnnotation(fexp);
        IntervalJoinUtils.updateJoinPlan(op, context, fexp, left, right, rangeAnnotation);
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
        PartitioningType leftPartitioningType = PartitioningType.ORDERED_PARTITIONED;
        PartitioningType rightPartitioningType = PartitioningType.ORDERED_PARTITIONED;
        if (fi.equals(BuiltinFunctions.INTERVAL_OVERLAPPED_BY)) {
            rightPartitioningType = PartitioningType.PARTIAL_BROADCAST_ORDERED_INTERSECT;
        } else if (fi.equals(BuiltinFunctions.INTERVAL_OVERLAPS)) {
            leftPartitioningType = PartitioningType.PARTIAL_BROADCAST_ORDERED_INTERSECT;
        } else if (fi.equals(BuiltinFunctions.INTERVAL_OVERLAPPING)) {
            leftPartitioningType = PartitioningType.PARTIAL_BROADCAST_ORDERED_INTERSECT;
            rightPartitioningType = PartitioningType.PARTIAL_BROADCAST_ORDERED_INTERSECT;
        } else if (fi.equals(BuiltinFunctions.INTERVAL_COVERS)) {
            leftPartitioningType = PartitioningType.PARTIAL_BROADCAST_ORDERED_INTERSECT;
        } else if (fi.equals(BuiltinFunctions.INTERVAL_COVERED_BY)) {
            rightPartitioningType = PartitioningType.PARTIAL_BROADCAST_ORDERED_INTERSECT;
        } else if (fi.equals(BuiltinFunctions.INTERVAL_BEFORE)) {
            leftPartitioningType = PartitioningType.PARTIAL_BROADCAST_ORDERED_FOLLOWING;
        } else if (fi.equals(BuiltinFunctions.INTERVAL_AFTER)) {
            rightPartitioningType = PartitioningType.PARTIAL_BROADCAST_ORDERED_FOLLOWING;
        } else {
            throw new CompilationException(ErrorCode.COMPILATION_ILLEGAL_STATE, fi.getName());
        }
        return new IntervalPartitions(leftIC, rightIC, leftPartitioningType, rightPartitioningType, rangeMap,
                rangeMapKey);
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
        ao.setExecutionMode(op.getExecutionMode());
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

    protected static Pair<MutableObject<ILogicalOperator>, MutableObject<ILogicalOperator>> replicateInputForForwardAndAggregateOperators(
            AbstractBinaryJoinOperator op, IOptimizationContext context, int side) throws AlgebricksException {

        Mutable<ILogicalOperator> leftInputOp = op.getInputs().get(side);
        // Create Left Replicate Operator
        SourceLocation sourceLocation = op.getSourceLocation();
        ReplicateOperator replicateOperator = new ReplicateOperator(2);
        replicateOperator.setPhysicalOperator(new ReplicatePOperator());
        replicateOperator.setSourceLocation(sourceLocation);
        replicateOperator.getInputs().add(new MutableObject<>(leftInputOp.getValue()));
        OperatorManipulationUtil.setOperatorMode(replicateOperator);
        replicateOperator.recomputeSchema();
        context.computeAndSetTypeEnvironmentForOperator(replicateOperator);

        ExchangeOperator exchToForward = createOneToOneExchangeOp(replicateOperator, context);
        MutableObject<ILogicalOperator> exchToForwardRef = new MutableObject<>(exchToForward);

        ExchangeOperator exchToLocalAgg = createOneToOneExchangeOp(replicateOperator, context);
        MutableObject<ILogicalOperator> exchToLocalAggRef = new MutableObject<>(exchToLocalAgg);

        replicateOperator.getOutputMaterializationFlags()[0] = true;

        return new Pair<>(exchToForwardRef, exchToLocalAggRef);
    }

    protected static Pair<ILogicalOperator, List<LogicalVariable>> createLocalIntervalRangeAggregate(
            AbstractBinaryJoinOperator op, IOptimizationContext context,
            MutableObject<ILogicalOperator> exchToLocalAggRef, List<LogicalVariable> side) throws AlgebricksException {

        AbstractLogicalExpression inputVarRef;
        List<Mutable<ILogicalExpression>> fields = new ArrayList<>(1);
        for (LogicalVariable inputVar : side) {
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
        return new Pair<>(localAggOperator, localAggResultVars);
    }

    protected static Pair<MutableObject<ILogicalOperator>, List<LogicalVariable>> createGlobalUnionRangeAggregateOperator(
            AbstractBinaryJoinOperator op, IOptimizationContext context, ILogicalOperator leftLocalAggregate,
            ILogicalOperator rightLocalAggregate, LogicalVariable leftLocalAggregateVariable,
            LogicalVariable rightLocalAggregateVariable) throws AlgebricksException {
        LogicalVariable unionRangeVar = context.newVar();
        Triple<LogicalVariable, LogicalVariable, LogicalVariable> unionVarMap =
                new Triple<>(leftLocalAggregateVariable, rightLocalAggregateVariable, unionRangeVar);
        List<Triple<LogicalVariable, LogicalVariable, LogicalVariable>> unionVarMaps = new ArrayList<>();
        unionVarMaps.add(unionVarMap);
        UnionAllOperator unionAllOperator = new UnionAllOperator(unionVarMaps);
        unionAllOperator.setSourceLocation(op.getSourceLocation());
        unionAllOperator.getInputs().add(new MutableObject<>(leftLocalAggregate));
        unionAllOperator.getInputs().add(new MutableObject<>(rightLocalAggregate));
        OperatorManipulationUtil.setOperatorMode(unionAllOperator);
        unionAllOperator.recomputeSchema();
        context.computeAndSetTypeEnvironmentForOperator(unionAllOperator);
        MutableObject<ILogicalOperator> unionAllOpRef = new MutableObject<>(unionAllOperator);
        // Compute the union range of the left and the right ranges
        return createGlobalAggregateOperator(op, context, unionRangeVar, unionAllOpRef);
    }

    protected static void buildSortMergeIntervalPlanWithDynamicHint(AbstractBinaryJoinOperator op,
            IOptimizationContext context, FunctionIdentifier fi, List<LogicalVariable> sideLeft,
            List<LogicalVariable> sideRight, int left, int right) throws AlgebricksException {

        // Left Side
        // Replicate Left Input
        Pair<MutableObject<ILogicalOperator>, MutableObject<ILogicalOperator>> leftExchanges =
                replicateInputForForwardAndAggregateOperators(op, context, left);
        // Exchanges
        MutableObject<ILogicalOperator> inputToLeftForwardOperator = leftExchanges.getFirst();
        MutableObject<ILogicalOperator> inputToLeftLocalAggregateOperator = leftExchanges.getSecond();
        // Create Left Local Aggregate Operator
        Pair<ILogicalOperator, List<LogicalVariable>> leftLocalAggregateInfo =
                createLocalIntervalRangeAggregate(op, context, inputToLeftLocalAggregateOperator, sideLeft);
        // Left Local Aggregate Operator
        ILogicalOperator leftLocalRangeAggregateOperator = leftLocalAggregateInfo.getFirst();
        LogicalVariable leftLocalRangeAggregateOperatorVar = leftLocalAggregateInfo.getSecond().get(0);

        // Right Side
        // Replicate Right Input
        Pair<MutableObject<ILogicalOperator>, MutableObject<ILogicalOperator>> rightExchanges =
                replicateInputForForwardAndAggregateOperators(op, context, right);
        // Exchanges
        MutableObject<ILogicalOperator> inputToRightForwardOperator = rightExchanges.getFirst();
        MutableObject<ILogicalOperator> inputToRightLocalAggregateOperator = rightExchanges.getSecond();
        // Create Right Local Aggregate Operator
        Pair<ILogicalOperator, List<LogicalVariable>> rightLocalAggregateInfo =
                createLocalIntervalRangeAggregate(op, context, inputToRightLocalAggregateOperator, sideRight);
        // Right Local Aggregate Operator
        ILogicalOperator rightLocalRangeAggregateOperator = rightLocalAggregateInfo.getFirst();
        LogicalVariable rightLocalRangeAggregateOperatorVar = rightLocalAggregateInfo.getSecond().get(0);

        // Create Union Range Operator
        Pair<MutableObject<ILogicalOperator>, List<LogicalVariable>> globalUnionAggregateOperatorInfo =
                createGlobalUnionRangeAggregateOperator(op, context, leftLocalRangeAggregateOperator,
                        rightLocalRangeAggregateOperator, leftLocalRangeAggregateOperatorVar,
                        rightLocalRangeAggregateOperatorVar);
        // Aggregate Union Range Operator
        MutableObject<ILogicalOperator> globalUnionAggregateOperator = globalUnionAggregateOperatorInfo.getFirst();
        LogicalVariable globalUnionAggregateOperatorVar = globalUnionAggregateOperatorInfo.getSecond().get(0);

        // Create Range Map Operator
        Pair<MutableObject<ILogicalOperator>, List<LogicalVariable>> rangeMapAggregateInfo =
                createRangeMapAggregate(op, context, globalUnionAggregateOperator, globalUnionAggregateOperatorVar);
        // Aggregate Union Range Operator
        MutableObject<ILogicalOperator> rangeMapAggregateOperator = rangeMapAggregateInfo.getFirst();
        LogicalVariable rangeMapAggregateOperatorVar = rangeMapAggregateInfo.getSecond().get(0);

        // Create a replicate operator to replicate Range Map
        ReplicateOperator unionRangeReplicateOperator =
                createReplicateOperator(rangeMapAggregateOperator, context, op.getSourceLocation(), 2);

        // Replicate the RangeMap points to the left branch
        ExchangeOperator exchRangeToForwardOpLeft = createBroadcastExchangeOp(unionRangeReplicateOperator, context);
        MutableObject<ILogicalOperator> exchRangeToForwardOpLeftRef = new MutableObject<>(exchRangeToForwardOpLeft);
        Pair<Mutable<ILogicalOperator>, LogicalVariable> leftRangeMapAggregateOperatorInfo =
                createAssignProjectOperator(op, rangeMapAggregateOperatorVar, unionRangeReplicateOperator,
                        exchRangeToForwardOpLeftRef, context);

        // Left Range Map Operator
        Mutable<ILogicalOperator> leftRangeMapAggregateOperator = leftRangeMapAggregateOperatorInfo.getFirst();
        LogicalVariable leftRangeMapAggregateVar = leftRangeMapAggregateOperatorInfo.getSecond();

        // Replicate RangeMap to the right branch
        ExchangeOperator exchRangeToForwardOpRight = createBroadcastExchangeOp(unionRangeReplicateOperator, context);
        MutableObject<ILogicalOperator> exchRangeToForwardOpRightRef = new MutableObject<>(exchRangeToForwardOpRight);
        Pair<Mutable<ILogicalOperator>, LogicalVariable> rightRangeMapAggregateOperatorInfo =
                createAssignProjectOperator(op, rangeMapAggregateOperatorVar, unionRangeReplicateOperator,
                        exchRangeToForwardOpRightRef, context);

        // Right Range Map Operator
        Mutable<ILogicalOperator> rightRangeMapAggregateOperator = rightRangeMapAggregateOperatorInfo.getFirst();
        LogicalVariable rightRangeMapAggregateVar = rightRangeMapAggregateOperatorInfo.getSecond();

        // Left Branch
        // Create the forward operator
        //LogicalVariable unionRangeVar = context.newVar();
        String rangeMapKeyLeft = UUID.randomUUID().toString();
        AbstractLogicalExpression rangeMapExpressionLeft =
                new VariableReferenceExpression(leftRangeMapAggregateVar, op.getSourceLocation());
        ForwardOperator forwardOperatorLeft =
                new ForwardOperator(rangeMapKeyLeft, new MutableObject<>(rangeMapExpressionLeft));
        forwardOperatorLeft.setSourceLocation(op.getSourceLocation());
        forwardOperatorLeft.setPhysicalOperator(new SortForwardPOperator());
        forwardOperatorLeft.getInputs().add(inputToLeftForwardOperator);
        OperatorManipulationUtil.setOperatorMode(forwardOperatorLeft);
        forwardOperatorLeft.recomputeSchema();
        context.computeAndSetTypeEnvironmentForOperator(forwardOperatorLeft);
        MutableObject<ILogicalOperator> leftForwardRef = new MutableObject<>(forwardOperatorLeft);

        // Right Branch
        // Create the forward operator
        String rangeMapKeyRight = UUID.randomUUID().toString();
        AbstractLogicalExpression rangeMapExpressionRight =
                new VariableReferenceExpression(rightRangeMapAggregateVar, op.getSourceLocation());
        ForwardOperator forwardOperatorRight =
                new ForwardOperator(rangeMapKeyRight, new MutableObject<>(rangeMapExpressionRight));
        forwardOperatorRight.setSourceLocation(op.getSourceLocation());
        forwardOperatorRight.setPhysicalOperator(new SortForwardPOperator());
        forwardOperatorRight.getInputs().add(inputToRightForwardOperator);
        OperatorManipulationUtil.setOperatorMode(forwardOperatorRight);
        forwardOperatorRight.recomputeSchema();
        context.computeAndSetTypeEnvironmentForOperator(forwardOperatorRight);
        MutableObject<ILogicalOperator> rightForwardRef = new MutableObject<>(forwardOperatorRight);

        op.getInputs().get(left).setValue(leftForwardRef.getValue());
        op.getInputs().get(right).setValue(rightForwardRef.getValue());

        IntervalPartitions intervalPartitions =
                createIntervalPartitions(op, fi, sideLeft, sideRight, context, left, right, null, rangeMapKeyRight);

        IIntervalJoinUtilFactory mjcf = createIntervalJoinCheckerFactory(fi, null, intervalPartitions.getRangeMapKey());
        op.setPhysicalOperator(new IntervalMergeJoinPOperator(op.getJoinKind(),
                AbstractJoinPOperator.JoinPartitioningType.BROADCAST, sideLeft, sideRight,
                context.getPhysicalOptimizationConfig().getMaxFramesForJoin(), mjcf, intervalPartitions));

        //        List<LogicalVariable> varsRight = ;
        //        ReplicateOperator rightForwardOut = createReplicateOperator(new MutableObject<>(forwardOperatorRight), context, op.getSourceLocation(), 1);
        //
        //        int numFields = 1;
        //        byte byteArray[] = new byte[12];
        //        int offsets[] = new int[3];
        //        offsets[0] = 0;
        //        offsets[1] = 4;
        //        offsets[2] = 8;
        //        double percentages[] = new double[3];
        //        RangeMap rangeMap = new RangeMap(numFields, byteArray, offsets, percentages);

    }

    private static Pair<MutableObject<ILogicalOperator>, List<LogicalVariable>> createRangeMapAggregate(
            AbstractBinaryJoinOperator op, IOptimizationContext context, Mutable<ILogicalOperator> inputOp,
            LogicalVariable inputVars) throws AlgebricksException {
        // Add ReplicationOperator for the input branch
        SourceLocation sourceLocation = op.getSourceLocation();
        ReplicateOperator replicateOperator = createReplicateOperator(inputOp, context, sourceLocation, 1);

        ExchangeOperator exchToLocalAgg = createOneToOneExchangeOp(replicateOperator, context);
        MutableObject<ILogicalOperator> exchToLocalAggRef = new MutableObject<>(exchToLocalAgg);

        // Materialize the data to be able to re-read the data again
        replicateOperator.getOutputMaterializationFlags()[0] = true;

        Pair<MutableObject<ILogicalOperator>, List<LogicalVariable>> createRangeMapAggregateResults =
                createRangeMapAggregateOperator(op, context, inputVars, exchToLocalAggRef);
        return new Pair<>(createRangeMapAggregateResults.first, createRangeMapAggregateResults.second);
    }

    private static Pair<MutableObject<ILogicalOperator>, List<LogicalVariable>> createRangeMapAggregateOperator(
            AbstractBinaryJoinOperator op, IOptimizationContext context, LogicalVariable inputVar,
            MutableObject<ILogicalOperator> inputOperator) throws AlgebricksException {
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

    private static Pair<Mutable<ILogicalOperator>, LogicalVariable> createAssignProjectOperator(
            AbstractBinaryJoinOperator op, LogicalVariable inputVar, ReplicateOperator unionMBRReplicateOperator,
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
        assignOperator.setExecutionMode(unionMBRReplicateOperator.getExecutionMode());
        assignOperator.setPhysicalOperator(new AssignPOperator());
        AbstractLogicalOperator projectOperator = new ProjectOperator(finalMBRLiveVars);
        projectOperator.setSourceLocation(op.getSourceLocation());
        projectOperator.setPhysicalOperator(new StreamProjectPOperator());
        projectOperator.setExecutionMode(unionMBRReplicateOperator.getExecutionMode());
        assignOperator.getInputs().add(exchMBRToForwardRef);
        projectOperator.getInputs().add(new MutableObject<>(assignOperator));

        context.computeAndSetTypeEnvironmentForOperator(assignOperator);
        assignOperator.recomputeSchema();
        context.computeAndSetTypeEnvironmentForOperator(projectOperator);
        projectOperator.recomputeSchema();
        Mutable<ILogicalOperator> projectOperatorRef = new MutableObject<>(projectOperator);

        return new Pair<>(projectOperatorRef, newFinalMbrVar);
    }
}

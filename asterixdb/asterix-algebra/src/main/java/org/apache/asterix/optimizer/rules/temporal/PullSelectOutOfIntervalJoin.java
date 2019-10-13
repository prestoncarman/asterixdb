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
package org.apache.asterix.optimizer.rules.temporal;

import java.util.HashSet;
import java.util.Set;

import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.rewriter.rules.PullSelectOutOfEqJoin;

public class PullSelectOutOfIntervalJoin extends PullSelectOutOfEqJoin {
    
    boolean foundInterval = false;
    private static final Set<FunctionIdentifier> TRANSLATABLE_INTERVALS = new HashSet<>();
    {
        TRANSLATABLE_INTERVALS.add(AsterixBuiltinFunctions.INTERVAL_OVERLAPS);
        TRANSLATABLE_INTERVALS.add(AsterixBuiltinFunctions.INTERVAL_OVERLAPPING);
        TRANSLATABLE_INTERVALS.add(AsterixBuiltinFunctions.INTERVAL_OVERLAPPED_BY);
        TRANSLATABLE_INTERVALS.add(AsterixBuiltinFunctions.INTERVAL_COVERED_BY);
        TRANSLATABLE_INTERVALS.add(AsterixBuiltinFunctions.INTERVAL_COVERS);
    }

    @Override
    protected boolean isEqVarVar(ILogicalExpression expr) {
        if (foundInterval) {
            return false;
        }
        if (expr.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }
        AbstractFunctionCallExpression f = (AbstractFunctionCallExpression) expr;
        if (TRANSLATABLE_INTERVALS.contains(f.getFunctionIdentifier())) {
            foundInterval = true;
            return true;
        }
        return false;
    }

}

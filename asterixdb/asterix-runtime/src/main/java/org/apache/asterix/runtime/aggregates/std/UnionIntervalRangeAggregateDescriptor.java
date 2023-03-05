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
package org.apache.asterix.runtime.aggregates.std;

public class UnionIntervalRangeAggregateDescriptor extends AbstractUnionIntervalRangeAggregateDescriptor {

    private static final long serialVersionUID = 1L;

    public static final org.apache.asterix.om.functions.IFunctionDescriptorFactory FACTORY =
            () -> new org.apache.asterix.runtime.aggregates.std.UnionIntervalRangeAggregateDescriptor();

    @Override
    public org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier getIdentifier() {
        return org.apache.asterix.om.functions.BuiltinFunctions.UNION_INTERVAL_RANGE;
    }

    @Override
    public org.apache.hyracks.algebricks.runtime.base.IAggregateEvaluatorFactory createAggregateEvaluatorFactory(
            final org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory[] args) {
        return new org.apache.hyracks.algebricks.runtime.base.IAggregateEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public org.apache.hyracks.algebricks.runtime.base.IAggregateEvaluator createAggregateEvaluator(
                    final org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext ctx)
                    throws org.apache.hyracks.api.exceptions.HyracksDataException {
                return new UnionIntervalRangeAggregateFunction(args, ctx, sourceLoc);
            }
        };
    }
}

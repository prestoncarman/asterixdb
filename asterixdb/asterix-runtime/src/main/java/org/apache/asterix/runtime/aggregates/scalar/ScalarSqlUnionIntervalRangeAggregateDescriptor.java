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
package org.apache.asterix.runtime.aggregates.scalar;

public class ScalarSqlUnionIntervalRangeAggregateDescriptor extends AbstractScalarAggregateDescriptor {

    private static final long serialVersionUID = 1L;

    public static final org.apache.asterix.om.functions.IFunctionDescriptorFactory FACTORY = createDescriptorFactory(
            org.apache.asterix.runtime.aggregates.scalar.ScalarSqlUnionIntervalRangeAggregateDescriptor::new);

    private ScalarSqlUnionIntervalRangeAggregateDescriptor() {
        super(org.apache.asterix.runtime.aggregates.std.SqlUnionIntervalRangeAggregateDescriptor.FACTORY);
    }

    @Override
    public org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier getIdentifier() {
        return org.apache.asterix.om.functions.BuiltinFunctions.SCALAR_SQL_UNION_INTERVAL_RANGE;
    }

    @Override
    public void setImmutableStates(Object... states) {
        super.setImmutableStates(states);
        aggFuncDesc.setImmutableStates(getItemType((org.apache.asterix.om.types.IAType) states[0]));
    }
}

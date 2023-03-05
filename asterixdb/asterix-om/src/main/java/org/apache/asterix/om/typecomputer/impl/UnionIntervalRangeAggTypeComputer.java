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
package org.apache.asterix.om.typecomputer.impl;

public class UnionIntervalRangeAggTypeComputer extends AggregateResultTypeComputer {

    public static final org.apache.asterix.om.typecomputer.impl.UnionIntervalRangeAggTypeComputer INSTANCE =
            new org.apache.asterix.om.typecomputer.impl.UnionIntervalRangeAggTypeComputer();

    private UnionIntervalRangeAggTypeComputer() {
    }

    @Override
    protected org.apache.asterix.om.types.IAType getResultType(
            org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression expr,
            org.apache.asterix.om.types.IAType... strippedInputTypes)
            throws org.apache.hyracks.algebricks.common.exceptions.AlgebricksException {
        org.apache.asterix.om.types.ATypeTag tag = strippedInputTypes[0].getTypeTag();
        switch (tag) {
            case RECTANGLE:
                return org.apache.asterix.om.types.AUnionType
                        .createNullableType(org.apache.asterix.om.types.BuiltinType.ARECTANGLE);
            case ANY:
                return org.apache.asterix.om.types.BuiltinType.ANY;
            default:
                // All other possible cases.
                return org.apache.asterix.om.types.BuiltinType.ANULL;
        }
    }
}

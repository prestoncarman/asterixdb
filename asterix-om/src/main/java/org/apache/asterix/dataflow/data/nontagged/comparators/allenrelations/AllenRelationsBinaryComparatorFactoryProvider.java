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
package org.apache.asterix.dataflow.data.nontagged.comparators.allenrelations;

import java.io.Serializable;

import org.apache.asterix.dataflow.data.nontagged.comparators.ABinaryComparator;
import org.apache.asterix.om.functions.AsterixBuiltinFunctions;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.data.IBinaryComparatorFactoryProvider;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class AllenRelationsBinaryComparatorFactoryProvider implements IBinaryComparatorFactoryProvider, Serializable {

    private static final long serialVersionUID = 1L;
    public static final AllenRelationsBinaryComparatorFactoryProvider INSTANCE = new AllenRelationsBinaryComparatorFactoryProvider();

    private AllenRelationsBinaryComparatorFactoryProvider() {
    }

    @Override
    public IBinaryComparatorFactory getBinaryComparatorFactory(Object type, boolean ascending) {
        // During a comparison, since proper type promotion among several numeric types are required,
        // we will use AObjectAscBinaryComparatorFactory, instead of using a specific comparator
        return OverlapIntervalBinaryComparatorFactory.INSTANCE;
    }

    public IBinaryComparatorFactory getBinaryComparatorFactory(FunctionIdentifier fid, boolean ascending) {
        return OverlapIntervalBinaryComparatorFactory.INSTANCE;
    }

    private IBinaryComparatorFactory addOffset(final IBinaryComparatorFactory inst, final boolean ascending) {
        return new IBinaryComparatorFactory() {

            private static final long serialVersionUID = 1L;

            @Override
            public IBinaryComparator createBinaryComparator() {
                final IBinaryComparator bc = inst.createBinaryComparator();
                if (ascending) {
                    return new ABinaryComparator() {

                        @Override
                        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
                                throws HyracksDataException {
                            return bc.compare(b1, s1 + 1, l1 - 1, b2, s2 + 1, l2 - 1);
                        }
                    };
                } else {
                    return new ABinaryComparator() {
                        @Override
                        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
                                throws HyracksDataException {
                            return -bc.compare(b1, s1 + 1, l1 - 1, b2, s2 + 1, l2 - 1);
                        }
                    };
                }
            }
        };
    }

}

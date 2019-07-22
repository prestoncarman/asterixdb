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

package org.apache.hyracks.tests.unit;

import org.apache.hyracks.api.dataflow.value.RangePartitioningType;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.partition.range.RangeMap;
import org.junit.Test;

public class FieldRangeProjectPartitionComputerFactoryTest extends AbstractFieldRangeMultiPartitionComputerFactoryTest {

    @Test
    public void testFRMPCF_Project_N4() throws HyracksDataException {
        int[][] results = new int[15][];
        results[0] = new int[] { 0 }; // -25
        results[1] = new int[] { 0 }; //  50
        results[2] = new int[] { 0 }; //  99
        results[3] = new int[] { 1 }; // 100
        results[4] = new int[] { 1 }; // 101
        results[5] = new int[] { 1 }; // 150
        results[6] = new int[] { 1 }; // 199
        results[7] = new int[] { 2 }; // 200
        results[8] = new int[] { 2 }; // 201
        results[9] = new int[] { 2 }; // 250
        results[10] = new int[] { 2 }; // 299
        results[11] = new int[] { 3 }; // 300
        results[12] = new int[] { 3 }; // 301
        results[13] = new int[] { 3 }; // 350
        results[14] = new int[] { 3 }; // 425

        RangeMap rangeMap = getRangeMap(MAP_POINTS);

        executeFieldRangeMultiPartitionTests(PARTITION_EDGE_CASES, rangeMap, BINARY_ASC_COMPARATOR_FACTORIES,
                RangePartitioningType.PROJECT, 4, results);

        // PROJECT_END and SPLIT are the same as PROJECT for Longs (Only changes for more complex types like intervals)

        executeFieldRangeMultiPartitionTests(PARTITION_EDGE_CASES, rangeMap, BINARY_ASC_COMPARATOR_FACTORIES,
                RangePartitioningType.PROJECT_END, 4, results);

        executeFieldRangeMultiPartitionTests(PARTITION_EDGE_CASES, rangeMap, BINARY_ASC_COMPARATOR_FACTORIES,
                RangePartitioningType.SPLIT, 4, results);
    }

    @Test
    public void testFRMPCF_Project_N16() throws HyracksDataException {
        int[][] results = new int[15][];
        results[0] = new int[] { 0 }; // -25
        results[1] = new int[] { 2 }; //  50
        results[2] = new int[] { 3 }; //  99
        results[3] = new int[] { 4 }; // 100
        results[4] = new int[] { 4 }; // 101
        results[5] = new int[] { 6 }; // 150
        results[6] = new int[] { 7 }; // 199
        results[7] = new int[] { 8 }; // 200
        results[8] = new int[] { 8 }; // 201
        results[9] = new int[] { 10 }; // 250
        results[10] = new int[] { 11 }; // 299
        results[11] = new int[] { 12 }; // 300
        results[12] = new int[] { 12 }; // 301
        results[13] = new int[] { 14 }; // 350
        results[14] = new int[] { 15 }; // 425

        RangeMap rangeMap = getRangeMap(MAP_POINTS);

        executeFieldRangeMultiPartitionTests(PARTITION_EDGE_CASES, rangeMap, BINARY_ASC_COMPARATOR_FACTORIES,
                RangePartitioningType.PROJECT, 16, results);

        // PROJECT_END and SPLIT are the same as PROJECT for Longs (Only changes for more complex types like intervals)

        executeFieldRangeMultiPartitionTests(PARTITION_EDGE_CASES, rangeMap, BINARY_ASC_COMPARATOR_FACTORIES,
                RangePartitioningType.PROJECT_END, 16, results);

        executeFieldRangeMultiPartitionTests(PARTITION_EDGE_CASES, rangeMap, BINARY_ASC_COMPARATOR_FACTORIES,
                RangePartitioningType.SPLIT, 16, results);
    }
}

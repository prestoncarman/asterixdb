package org.apache.hyracks.tests.unit;

import org.apache.hyracks.api.dataflow.value.RangePartitioningType;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.partition.range.RangeMap;
import org.junit.Test;

public class FieldRangeReplicatePartitionComputerFactoryTest extends AbstractFieldRangeMultiPartitionComputerFactoryTest {

    @Test public void testFRMPCF_Replicate_N4() throws HyracksDataException {
        int[][] results = new int[15][];
        results[0] = new int[] { 0, 1, 2, 3 }; // -25
        results[1] = new int[] { 0, 1, 2, 3 }; //  50
        results[2] = new int[] { 0, 1, 2, 3 }; //  99
        results[3] = new int[] { 1, 2, 3 }; // 100
        results[4] = new int[] { 1, 2, 3 }; // 101
        results[5] = new int[] { 1, 2, 3 }; // 150
        results[6] = new int[] { 1, 2, 3 }; // 199
        results[7] = new int[] { 2, 3 }; // 200
        results[8] = new int[] { 2, 3 }; // 201
        results[9] = new int[] { 2, 3 }; // 250
        results[10] = new int[] { 2, 3 }; // 299
        results[11] = new int[] { 3 }; // 300
        results[12] = new int[] { 3 }; // 301
        results[13] = new int[] { 3 }; // 350
        results[14] = new int[] { 3 }; // 425

        RangeMap rangeMap = getRangeMap(MAP_POINTS);

        executeFieldRangeMultiPartitionTests(PARTITION_EDGE_CASES, rangeMap,
                BINARY_ASC_COMPARATOR_FACTORIES, RangePartitioningType.REPLICATE, 4, results);
    }

    @Test public void testFRMPCF_Replicate_N16() throws HyracksDataException {
        int[][] results = new int[15][];
        results[0] = new int[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 }; // -25
        results[1] = new int[] { 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 }; //  50
        results[2] = new int[] { 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 }; //  99
        results[3] = new int[] { 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 }; // 100
        results[4] = new int[] { 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 }; // 101
        results[5] = new int[] { 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 }; // 150
        results[6] = new int[] { 7, 8, 9, 10, 11, 12, 13, 14, 15 }; // 199
        results[7] = new int[] { 8, 9, 10, 11, 12, 13, 14, 15 }; // 200
        results[8] = new int[] { 8, 9, 10, 11, 12, 13, 14, 15 }; // 201
        results[9] = new int[] { 10, 11, 12, 13, 14, 15 }; // 250
        results[10] = new int[] { 11, 12, 13, 14, 15 }; // 299
        results[11] = new int[] { 12, 13, 14, 15 }; // 300
        results[12] = new int[] { 12, 13, 14, 15 }; // 301
        results[13] = new int[] { 14, 15 }; // 350
        results[14] = new int[] { 15 }; // 425

        RangeMap rangeMap = getRangeMap(MAP_POINTS);

        executeFieldRangeMultiPartitionTests(PARTITION_EDGE_CASES, rangeMap,
                BINARY_ASC_COMPARATOR_FACTORIES, RangePartitioningType.REPLICATE, 16, results);
    }
}
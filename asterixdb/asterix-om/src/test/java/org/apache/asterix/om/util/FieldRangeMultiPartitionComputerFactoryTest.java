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

package org.apache.asterix.om.util;

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.asterix.dataflow.data.nontagged.comparators.AIntervalAscPartialBinaryComparatorFactory;
import org.apache.asterix.dataflow.data.nontagged.comparators.AIntervalDescPartialBinaryComparatorFactory;
import org.apache.asterix.dataflow.data.nontagged.comparators.AIntervalEndpointAscPartialBinaryComparatorFactory;
import org.apache.asterix.dataflow.data.nontagged.comparators.AIntervalStartpointDescPartialBinaryComparatorFactory;
import org.apache.asterix.dataflow.data.nontagged.serde.AIntervalSerializerDeserializer;
import org.apache.asterix.om.base.AInterval;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.IRangePartitionType.RangePartitioningType;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITupleMultiPartitionComputer;
import org.apache.hyracks.api.dataflow.value.ITupleMultiPartitionComputerFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.storage.IGrowableIntArray;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.dataflow.common.data.marshalling.Integer64SerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.partition.range.RangeMap;
import org.apache.hyracks.dataflow.common.data.partition.range.StaticFieldRangeMultiPartitionComputerFactory;
import org.apache.hyracks.storage.common.arraylist.IntArrayList;
import org.apache.hyracks.test.support.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import junit.framework.TestCase;

public class FieldRangeMultiPartitionComputerFactoryTest extends TestCase {

    private final AIntervalSerializerDeserializer intervalSerde = AIntervalSerializerDeserializer.INSTANCE;
    @SuppressWarnings("rawtypes")
    private final ISerializerDeserializer[] SerDers =
            new ISerializerDeserializer[] { Integer64SerializerDeserializer.INSTANCE };
    private final RecordDescriptor RecordDesc = new RecordDescriptor(SerDers);

    IBinaryComparatorFactory[] BINARY_ASC_COMPARATOR_FACTORIES =
            new IBinaryComparatorFactory[] { AIntervalAscPartialBinaryComparatorFactory.INSTANCE };
    IBinaryComparatorFactory[] BINARY_DESC_COMPARATOR_FACTORIES =
            new IBinaryComparatorFactory[] { AIntervalDescPartialBinaryComparatorFactory.INSTANCE };
    IBinaryComparatorFactory[] BINARY_ASC_MAX_COMPARATOR_FACTORIES =
            new IBinaryComparatorFactory[] { AIntervalEndpointAscPartialBinaryComparatorFactory.INSTANCE };
    IBinaryComparatorFactory[] BINARY_DESC_MAX_COMPARATOR_FACTORIES =
            new IBinaryComparatorFactory[] { AIntervalStartpointDescPartialBinaryComparatorFactory.INSTANCE };

    //    IBinaryRangeComparatorFactory[] BINARY_REPLICATE_COMPARATOR_FACTORIES = new IBinaryRangeComparatorFactory[] {
    //            new PointableBinaryReplicateRangeComparatorFactory(LongPointable.FACTORY) };
    /*
     * The following points (X) will be tested for these 4 partitions.
     *
     * X-------X----XXX----X----XXX----X----XXX----X-------X
     *    -----------|-----------|-----------|-----------
     *
     * The following points (X) will be tested for these 16 partitions.
     *
     * X-------X----XXX----X----XXX----X----XXX----X-------X
     *    --|--|--|--|--|--|--|--|--|--|--|--|--|--|--|--
     */

    private final int FRAME_SIZE = 640;
    private final int INTEGER_LENGTH = Long.BYTES;
    // tag=1 + start=INTEGER_LENGTH + end=INTEGER_LENGTH in bytes
    private final int INTERVAL_LENGTH = 1 + 2 * INTEGER_LENGTH;

    // Tests points inside each partition.
    //result index {      0,   1,   2,   3,    4,    5,    6,    7,    8,    9,   10,   11,   12,   13,   14,   15   };
    //points       {     20l, 45l, 70l, 95l, 120l, 145l, 170l, 195l, 220l, 245l, 270l, 295l, 320l, 345l, 370l, 395l  };
    private final Long[] EACH_PARTITION =
            new Long[] { 20l, 45l, 70l, 95l, 120l, 145l, 170l, 195l, 220l, 245l, 270l, 295l, 320l, 345l, 370l, 395l };

    // Tests points at or near partition boundaries and at the ends of the partition range.
    //result index {      0,   1,   2,   3,    4,    5,    6,    7,    8,    9,    10,   11,   12,   13,   14        };
    //points       {    -25l, 50l, 99l, 100l, 101l, 150l, 199l, 200l, 201l, 250l, 299l, 300l, 301l, 350l, 425l       };
    private final Long[] PARTITION_EDGE_CASES =
            new Long[] { -25l, 50l, 99l, 100l, 101l, 150l, 199l, 200l, 201l, 250l, 299l, 300l, 301l, 350l, 425l };

    // The map of the partitions, listed as the split points. split points include min and max.
    // first and last partitions include all values less than and greater than min and max split points respectively.
    //
    //             0          )[           1          )[           2            )[             3
    //   0  )[  1 )[  2 )[  3 )[  4 )[  5 )[  6 )[  7 )[  8 )[  9 )[  10 )[  11 )[  12 )[  13 )[  14 )[  15
    // 0     25    50    75    100   125   150   175   200   225   250    275    300    325    350    375    400
    // 400   375   350   325   300   275   250   225   200   175   150    125    100    75     50     25     0
    //   0   ]  (1 ]  (2 ]  (3 ]  (4 ]  (5 ]  (6 ]  (7 ]  (8 ]  (9 ]  (10 ]  (11 ]  (12 ] ( 13 ] ( 14 ] (  15
    //              0          ]  (           1        ]  (           2          ]  (             3
    // ^ last two rows: alternative map for descending

    // The map of the partitions, listed as the split points.
    // partitions   {  0,   1,   2,   3,    4,    5,    6,    7,    8,    9,   10,   11,   12,   13,   14,   15,   16 };
    // map          { 0l, 25l, 50l, 75l, 100l, 125l, 150l, 175l, 200l, 225l, 250l, 275l, 300l, 325l, 350l, 375l, 400l };
    private final Long[] MAP_POINTS = new Long[] { 0l, 25l, 50l, 75l, 100l, 125l, 150l, 175l, 200l, 225l, 250l, 275l,
            300l, 325l, 350l, 375l, 400l };

    /**
     * @param integers
     * @param duration
     * @return
     * @throws HyracksDataException
     */
    private byte[] getIntervalBytes(Long[] integers, long duration) throws HyracksDataException {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutput dos = new DataOutputStream(bos);
            AInterval[] intervals = getAIntervals(integers, duration);
            for (int i = 0; i < integers.length; ++i) {
                intervalSerde.serialize(intervals[i], dos);
            }
            bos.close();
            return bos.toByteArray();
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    private RangeMap getRangeMap(Long[] integers) throws HyracksDataException {
        int offsets[] = new int[integers.length];
        for (int i = 0; i < integers.length; ++i) {
            offsets[i] = (i + 1) * INTERVAL_LENGTH;
        }
        return new RangeMap(1, getIntervalBytes(integers, 0), offsets);
    }

    private AInterval[] getAIntervals(Long[] integers, long duration) {
        AInterval[] intervals = new AInterval[integers.length];
        for (int i = 0; i < integers.length; ++i) {
            intervals[i] = new AInterval(integers[i], integers[i] + duration, ATypeTag.SERIALIZED_DATETIME_TYPE_TAG);
        }
        return intervals;
    }

    private ByteBuffer prepareData(IHyracksTaskContext ctx, AInterval[] intervals) throws HyracksDataException {
        IFrame frame = new VSizeFrame(ctx);

        FrameTupleAppender appender = new FrameTupleAppender();
        ArrayTupleBuilder tb = new ArrayTupleBuilder(RecordDesc.getFieldCount());
        DataOutput dos = tb.getDataOutput();
        appender.reset(frame, true);

        for (int i = 0; i < intervals.length; ++i) {
            tb.reset();
            intervalSerde.serialize(intervals[i], dos);
            tb.addFieldEndOffset();
            appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize());
        }

        return frame.getBuffer();
    }

    private void executeFieldRangeMultiPartitionTests(Long[] integers, RangeMap rangeMap,
            IBinaryComparatorFactory[] minComparatorFactories, IBinaryComparatorFactory[] maxComparatorFactories,
            RangePartitioningType rangeType, int nParts, int[][] results, long duration) throws HyracksDataException {
        IHyracksTaskContext ctx = TestUtils.create(FRAME_SIZE);
        int[] rangeFields = new int[] { 0 };
        ITupleMultiPartitionComputerFactory itmpcf = new StaticFieldRangeMultiPartitionComputerFactory(rangeFields,
                minComparatorFactories, maxComparatorFactories, rangeMap, rangeType);
        ITupleMultiPartitionComputer partitioner = itmpcf.createPartitioner(ctx);
        partitioner.initialize();

        IFrameTupleAccessor accessor = new FrameTupleAccessor(RecordDesc);
        AInterval[] intervals = getAIntervals(integers, duration);
        ByteBuffer buffer = prepareData(ctx, intervals);
        accessor.reset(buffer);

        IGrowableIntArray map = new IntArrayList(16, 1);

        for (int i = 0; i < results.length; ++i) {
            map.clear();
            partitioner.partition(accessor, i, nParts, map);
            checkPartitionResult(intervals[i], results[i], map);
        }
    }

    private String getString(int[] results) {
        String result = "[";
        for (int i = 0; i < results.length; ++i) {
            result += results[i];
            if (i < results.length - 1) {
                result += ", ";
            }
        }
        result += "]";
        return result;
    }

    private String getString(IGrowableIntArray results) {
        String result = "[";
        for (int i = 0; i < results.size(); ++i) {
            result += results.get(i);
            if (i < results.size() - 1) {
                result += ", ";
            }
        }
        result += "]";
        return result;
    }

    private void checkPartitionResult(AInterval interval, int[] results, IGrowableIntArray map) {
        if (results.length != map.size()) {
            Assert.assertEquals("The partition for value (" + interval.getIntervalStart() + ":"
                    + interval.getIntervalEnd() + ") gives different number of partitions", results.length, map.size());
        }
        for (int i = 0; i < results.length; ++i) {
            boolean match = false;
            for (int j = 0; j < results.length; ++j) {
                if (results[i] == map.get(j)) {
                    match = true;
                    continue;
                }
            }
            if (!match) {
                Assert.assertEquals("Individual partitions for " + interval + " do not match", getString(results),
                        getString(map));
                return;
            }
        }
    }

    // ============================
    // PROJECT TESTS

    @Test // (Stephen) Results Array Checked
    public void testFRMPCF_Project_ASC_D3_N4_EDGE() throws HyracksDataException {
        int[][] results = new int[15][];
        results[0] = new int[] { 0 };
        results[1] = new int[] { 0 };
        results[2] = new int[] { 0 };
        results[3] = new int[] { 1 };
        results[4] = new int[] { 1 };
        results[5] = new int[] { 1 };
        results[6] = new int[] { 1 };
        results[7] = new int[] { 2 };
        results[8] = new int[] { 2 };
        results[9] = new int[] { 2 };
        results[10] = new int[] { 2 };
        results[11] = new int[] { 3 };
        results[12] = new int[] { 3 };
        results[13] = new int[] { 3 };
        results[14] = new int[] { 3 };

        RangeMap rangeMap = getRangeMap(MAP_POINTS);

        executeFieldRangeMultiPartitionTests(PARTITION_EDGE_CASES, rangeMap, BINARY_ASC_COMPARATOR_FACTORIES,
                BINARY_ASC_COMPARATOR_FACTORIES, RangePartitioningType.PROJECT, 4, results, 3);
    }

    @Test // (Stephen) Results Array Checked, would like a double check
    public void testFRMPCF_Project_DESC_D3_N4_EDGE() throws HyracksDataException {
        int[][] results = new int[15][];
        results[0] = new int[] { 3 }; // -25:-22
        results[1] = new int[] { 3 }; //  50:53
        results[2] = new int[] { 2 }; //  99:102
        results[3] = new int[] { 2 }; // 100:103
        results[4] = new int[] { 2 }; // 101:104
        results[5] = new int[] { 2 }; // 150:153
        results[6] = new int[] { 1 }; // 199:202
        results[7] = new int[] { 1 }; // 200:203
        results[8] = new int[] { 1 }; // 201:204
        results[9] = new int[] { 1 }; // 250:253
        results[10] = new int[] { 0 }; // 299:302
        results[11] = new int[] { 0 }; // 300:303
        results[12] = new int[] { 0 }; // 301:304
        results[13] = new int[] { 0 }; // 350:353
        results[14] = new int[] { 0 }; // 425:428

        Long[] map = MAP_POINTS.clone();
        ArrayUtils.reverse(map);
        RangeMap rangeMap = getRangeMap(map);

        executeFieldRangeMultiPartitionTests(PARTITION_EDGE_CASES, rangeMap, BINARY_DESC_COMPARATOR_FACTORIES,
                BINARY_DESC_COMPARATOR_FACTORIES, RangePartitioningType.PROJECT, 4, results, 3);
    }

    // ============================
    // deprecated PROJECT TESTS

    @Test
    public void testFieldRangeMultiPartitionAscProject4AllPartitions() throws HyracksDataException {
        int[][] results = new int[16][];
        results[0] = new int[] { 0 };
        results[1] = new int[] { 0 };
        results[2] = new int[] { 0 };
        results[3] = new int[] { 0 };
        results[4] = new int[] { 1 };
        results[5] = new int[] { 1 };
        results[6] = new int[] { 1 };
        results[7] = new int[] { 1 };
        results[8] = new int[] { 2 };
        results[9] = new int[] { 2 };
        results[10] = new int[] { 2 };
        results[11] = new int[] { 2 };
        results[12] = new int[] { 3 };
        results[13] = new int[] { 3 };
        results[14] = new int[] { 3 };
        results[15] = new int[] { 3 };

        RangeMap rangeMap = getRangeMap(MAP_POINTS);

        executeFieldRangeMultiPartitionTests(EACH_PARTITION, rangeMap, BINARY_ASC_COMPARATOR_FACTORIES,
                BINARY_ASC_COMPARATOR_FACTORIES, RangePartitioningType.PROJECT, 4, results, 3);
    }

    @Test
    public void testFieldRangeMultiPartitionDescProject4AllPartitions() throws HyracksDataException {
        int[][] results = new int[16][];
        results[0] = new int[] { 3 };
        results[1] = new int[] { 3 };
        results[2] = new int[] { 3 };
        results[3] = new int[] { 3 };
        results[4] = new int[] { 2 };
        results[5] = new int[] { 2 };
        results[6] = new int[] { 2 };
        results[7] = new int[] { 2 };
        results[8] = new int[] { 1 };
        results[9] = new int[] { 1 };
        results[10] = new int[] { 1 };
        results[11] = new int[] { 1 };
        results[12] = new int[] { 0 };
        results[13] = new int[] { 0 };
        results[14] = new int[] { 0 };
        results[15] = new int[] { 0 };

        Long[] map = MAP_POINTS.clone();
        ArrayUtils.reverse(map);
        RangeMap rangeMap = getRangeMap(map);

        executeFieldRangeMultiPartitionTests(EACH_PARTITION, rangeMap, BINARY_DESC_COMPARATOR_FACTORIES,
                BINARY_DESC_COMPARATOR_FACTORIES, RangePartitioningType.PROJECT, 4, results, 3);
    }

    @Test
    public void testFieldRangeMultiPartitionAscProject16AllPartitions() throws HyracksDataException {
        int[][] results = new int[16][];
        results[0] = new int[] { 0 };
        results[1] = new int[] { 1 };
        results[2] = new int[] { 2 };
        results[3] = new int[] { 3 };
        results[4] = new int[] { 4 };
        results[5] = new int[] { 5 };
        results[6] = new int[] { 6 };
        results[7] = new int[] { 7 };
        results[8] = new int[] { 8 };
        results[9] = new int[] { 9 };
        results[10] = new int[] { 10 };
        results[11] = new int[] { 11 };
        results[12] = new int[] { 12 };
        results[13] = new int[] { 13 };
        results[14] = new int[] { 14 };
        results[15] = new int[] { 15 };

        RangeMap rangeMap = getRangeMap(MAP_POINTS);

        executeFieldRangeMultiPartitionTests(EACH_PARTITION, rangeMap, BINARY_ASC_COMPARATOR_FACTORIES,
                BINARY_ASC_COMPARATOR_FACTORIES, RangePartitioningType.PROJECT, 16, results, 3);
    }

    @Test
    public void testFieldRangeMultiPartitionDescProject16AllPartitions() throws HyracksDataException {
        int[][] results = new int[16][];
        results[0] = new int[] { 15 };
        results[1] = new int[] { 14 };
        results[2] = new int[] { 13 };
        results[3] = new int[] { 12 };
        results[4] = new int[] { 11 };
        results[5] = new int[] { 10 };
        results[6] = new int[] { 9 };
        results[7] = new int[] { 8 };
        results[8] = new int[] { 7 };
        results[9] = new int[] { 6 };
        results[10] = new int[] { 5 };
        results[11] = new int[] { 4 };
        results[12] = new int[] { 3 };
        results[13] = new int[] { 2 };
        results[14] = new int[] { 1 };
        results[15] = new int[] { 0 };

        Long[] map = MAP_POINTS.clone();
        ArrayUtils.reverse(map);
        RangeMap rangeMap = getRangeMap(map);

        executeFieldRangeMultiPartitionTests(EACH_PARTITION, rangeMap, BINARY_DESC_COMPARATOR_FACTORIES,
                BINARY_DESC_COMPARATOR_FACTORIES, RangePartitioningType.PROJECT, 16, results, 3);
    }

    @Test // (Stephen) Results Array Checked
    public void testFieldRangeMultiPartitionAscProject4Partitions() throws HyracksDataException {
        int[][] results = new int[15][];
        results[0] = new int[] { 0 };
        results[1] = new int[] { 0 };
        results[2] = new int[] { 0 };
        results[3] = new int[] { 1 };
        results[4] = new int[] { 1 };
        results[5] = new int[] { 1 };
        results[6] = new int[] { 1 };
        results[7] = new int[] { 2 };
        results[8] = new int[] { 2 };
        results[9] = new int[] { 2 };
        results[10] = new int[] { 2 };
        results[11] = new int[] { 3 };
        results[12] = new int[] { 3 };
        results[13] = new int[] { 3 };
        results[14] = new int[] { 3 };

        RangeMap rangeMap = getRangeMap(MAP_POINTS);

        executeFieldRangeMultiPartitionTests(PARTITION_EDGE_CASES, rangeMap, BINARY_ASC_COMPARATOR_FACTORIES,
                BINARY_ASC_COMPARATOR_FACTORIES, RangePartitioningType.PROJECT, 4, results, 3);
    }

    @Test // (Stephen) Results Array Checked
    public void testFieldRangeMultiPartitionAscProject16Partitions() throws HyracksDataException {
        int[][] results = new int[15][];
        results[0] = new int[] { 0 };
        results[1] = new int[] { 2 };
        results[2] = new int[] { 3 };
        results[3] = new int[] { 4 };
        results[4] = new int[] { 4 };
        results[5] = new int[] { 6 };
        results[6] = new int[] { 7 };
        results[7] = new int[] { 8 };
        results[8] = new int[] { 8 };
        results[9] = new int[] { 10 };
        results[10] = new int[] { 11 };
        results[11] = new int[] { 12 };
        results[12] = new int[] { 12 };
        results[13] = new int[] { 14 };
        results[14] = new int[] { 15 }; // Actual: [16]

        RangeMap rangeMap = getRangeMap(MAP_POINTS);

        executeFieldRangeMultiPartitionTests(PARTITION_EDGE_CASES, rangeMap, BINARY_ASC_COMPARATOR_FACTORIES,
                BINARY_ASC_COMPARATOR_FACTORIES, RangePartitioningType.PROJECT, 16, results, 3);
    }

    // ============================
    // PROJECT_END TESTS

    //    @Test
    //    public void testFRMPCF_ProjectEnd_ASC_D3_N4_EDGE() {}
    //
    //    @Test
    //    public void testFRMPCF_ProjectEnd_DESC_D3_N4_EDGE() {}

    // ============================
    // REPLICATE TESTS

    @Test // (Stephen) Results Array Checked
    public void testFRMPCF_Replicate_ASC_D3_N4_EDGE() throws HyracksDataException {
        int[][] results = new int[15][];
        results[0] = new int[] { 0, 1, 2, 3 };
        results[1] = new int[] { 0, 1, 2, 3 };
        results[2] = new int[] { 0, 1, 2, 3 };
        results[3] = new int[] { 1, 2, 3 };
        results[4] = new int[] { 1, 2, 3 };
        results[5] = new int[] { 1, 2, 3 };
        results[6] = new int[] { 1, 2, 3 };
        results[7] = new int[] { 2, 3 };
        results[8] = new int[] { 2, 3 };
        results[9] = new int[] { 2, 3 };
        results[10] = new int[] { 2, 3 };
        results[11] = new int[] { 3 };
        results[12] = new int[] { 3 };
        results[13] = new int[] { 3 };
        results[14] = new int[] { 3 };

        RangeMap rangeMap = getRangeMap(MAP_POINTS);

        executeFieldRangeMultiPartitionTests(PARTITION_EDGE_CASES, rangeMap, BINARY_ASC_COMPARATOR_FACTORIES,
                BINARY_ASC_COMPARATOR_FACTORIES, RangePartitioningType.REPLICATE, 4, results, 3);
    }

    @Test // (Stephen) Results Array Checked, unsure of direction in which Replicate extends for DESC
    public void testFRMPCF_Replicate_DESC_D3_N4_EDGE() throws HyracksDataException {
        int[][] results = new int[15][];
        results[0] = new int[] { 3 }; // -25
        results[1] = new int[] { 3 }; //  50
        results[2] = new int[] { 3 }; //  99
        results[3] = new int[] { 3 }; // 100
        results[4] = new int[] { 2, 3 }; // 101
        results[5] = new int[] { 2, 3 }; // 150
        results[6] = new int[] { 2, 3 }; // 199
        results[7] = new int[] { 2, 3 }; // 200
        results[8] = new int[] { 1, 2, 3 }; // 201
        results[9] = new int[] { 1, 2, 3 }; // 250
        results[10] = new int[] { 1, 2, 3 }; // 299
        results[11] = new int[] { 1, 2, 3 }; // 300
        results[12] = new int[] { 0, 1, 2, 3 }; // 301
        results[13] = new int[] { 0, 1, 2, 3 }; // 350
        results[14] = new int[] { 0, 1, 2, 3 }; // 425

        Long[] map = MAP_POINTS.clone();
        ArrayUtils.reverse(map);
        RangeMap rangeMap = getRangeMap(map);

        executeFieldRangeMultiPartitionTests(PARTITION_EDGE_CASES, rangeMap, BINARY_DESC_COMPARATOR_FACTORIES,
                BINARY_DESC_COMPARATOR_FACTORIES, RangePartitioningType.REPLICATE, 4, results, 3);
    }

    // ============================
    // deprecated REPLICATE TESTS

    @Test // (Stephen) Results Array Checked
    public void testFieldRangeMultiPartitionAscReplicate16Partitions() throws HyracksDataException {
        int[][] results = new int[15][];
        results[0] = new int[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };
        results[1] = new int[] { 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };
        results[2] = new int[] { 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };
        results[3] = new int[] { 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };
        results[4] = new int[] { 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };
        results[5] = new int[] { 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };
        results[6] = new int[] { 7, 8, 9, 10, 11, 12, 13, 14, 15 };
        results[7] = new int[] { 8, 9, 10, 11, 12, 13, 14, 15 };
        results[8] = new int[] { 8, 9, 10, 11, 12, 13, 14, 15 };
        results[9] = new int[] { 10, 11, 12, 13, 14, 15 };
        results[10] = new int[] { 11, 12, 13, 14, 15 };
        results[11] = new int[] { 12, 13, 14, 15 };
        results[12] = new int[] { 12, 13, 14, 15 };
        results[13] = new int[] { 14, 15 };
        results[14] = new int[] { 15 };

        RangeMap rangeMap = getRangeMap(MAP_POINTS);

        executeFieldRangeMultiPartitionTests(PARTITION_EDGE_CASES, rangeMap, BINARY_ASC_COMPARATOR_FACTORIES,
                BINARY_ASC_COMPARATOR_FACTORIES, RangePartitioningType.REPLICATE, 16, results, 3);
    }

    // ============================
    // SPLIT TESTS

    @Test // (Stephen) Results Array Checked
    public void testFRMPCF_Split_ASC_D3_N4_EDGE() throws HyracksDataException {
        int[][] results = new int[15][];
        results[0] = new int[] { 0 };
        results[1] = new int[] { 0 };
        results[2] = new int[] { 0, 1 };
        results[3] = new int[] { 1 };
        results[4] = new int[] { 1 };
        results[5] = new int[] { 1 };
        results[6] = new int[] { 1, 2 };
        results[7] = new int[] { 2 };
        results[8] = new int[] { 2 };
        results[9] = new int[] { 2 };
        results[10] = new int[] { 2, 3 };
        results[11] = new int[] { 3 };
        results[12] = new int[] { 3 };
        results[13] = new int[] { 3 };
        results[14] = new int[] { 3 };

        RangeMap rangeMap = getRangeMap(MAP_POINTS);

        executeFieldRangeMultiPartitionTests(PARTITION_EDGE_CASES, rangeMap, BINARY_ASC_COMPARATOR_FACTORIES,
                BINARY_ASC_MAX_COMPARATOR_FACTORIES, RangePartitioningType.SPLIT, 4, results, 3);
    }

    @Test // (Stephen) Results Array Checked, would like a double check
    public void testFRMPCF_Split_DESC_D3_N4_EDGE() throws HyracksDataException {
        int[][] results = new int[15][];
        results[0] = new int[] { 3 }; // -25:-22
        results[1] = new int[] { 3 }; //  50:53
        results[2] = new int[] { 2, 3 }; //  99:102
        results[3] = new int[] { 2, 3 }; // 100:103
        results[4] = new int[] { 2 }; // 101:104
        results[5] = new int[] { 2 }; // 150:153
        results[6] = new int[] { 1, 2 }; // 199:202
        results[7] = new int[] { 1, 2 }; // 200:203
        results[8] = new int[] { 1 }; // 201:204
        results[9] = new int[] { 1 }; // 250:253
        results[10] = new int[] { 0, 1 }; // 299:302
        results[11] = new int[] { 0, 1 }; // 300:303
        results[12] = new int[] { 0 }; // 301:304
        results[13] = new int[] { 0 }; // 350:353
        results[14] = new int[] { 0 }; // 425:428

        Long[] map = MAP_POINTS.clone();
        ArrayUtils.reverse(map);
        RangeMap rangeMap = getRangeMap(map);

        executeFieldRangeMultiPartitionTests(PARTITION_EDGE_CASES, rangeMap, BINARY_DESC_MAX_COMPARATOR_FACTORIES,
                BINARY_DESC_COMPARATOR_FACTORIES, RangePartitioningType.SPLIT, 4, results, 3);
    }

    @Test // (Stephen) Results Array Checked
    public void testFRMPCF_Split_ASC_D50_N16_EDGE() throws HyracksDataException {
        int[][] results = new int[15][];
        results[0] = new int[] { 0, 1 }; // -25:25  // Actual: 1 partition
        results[1] = new int[] { 2, 3, 4 }; // 50:100
        results[2] = new int[] { 3, 4, 5 }; // 99:149
        results[3] = new int[] { 4, 5, 6 }; // 100:150
        results[4] = new int[] { 4, 5, 6 }; // 101:151
        results[5] = new int[] { 6, 7, 8 }; // 150:200
        results[6] = new int[] { 7, 8, 9 }; // 199:249
        results[7] = new int[] { 8, 9, 10 }; // 200:250
        results[8] = new int[] { 8, 9, 10 }; // 201:251
        results[9] = new int[] { 10, 11, 12 }; // 250:300
        results[10] = new int[] { 11, 12, 13 }; // 299:349
        results[11] = new int[] { 12, 13, 14 }; // 300:350
        results[12] = new int[] { 12, 13, 14 }; // 301:351
        results[13] = new int[] { 14, 15 }; // 350:400
        results[14] = new int[] { 15 }; // 425:475

        RangeMap rangeMap = getRangeMap(MAP_POINTS);

        executeFieldRangeMultiPartitionTests(PARTITION_EDGE_CASES, rangeMap, BINARY_ASC_COMPARATOR_FACTORIES,
                BINARY_ASC_MAX_COMPARATOR_FACTORIES, RangePartitioningType.SPLIT, 16, results, 50);
    }

    @Test // (Stephen) Results Array Checked, would like a double check
    public void testFRMPCF_Split_DESC_D50_N16_EDGE() throws HyracksDataException {
        int[][] results = new int[15][];
        results[0] = new int[] { 15 }; // -25:25
        results[1] = new int[] { 12, 13 }; // 50:100  // Actual: 2 partitions
        results[2] = new int[] { 10, 11, 12 }; // 99:149
        results[3] = new int[] { 10, 11, 12 }; // 100:150
        results[4] = new int[] { 9, 10, 11 }; // 101:151
        results[5] = new int[] { 8, 9, 10 }; // 150:200
        results[6] = new int[] { 6, 7, 8 }; // 199:249
        results[7] = new int[] { 6, 7, 8 }; // 200:250
        results[8] = new int[] { 5, 6, 7 }; // 201:251
        results[9] = new int[] { 4, 5, 6 }; // 250:300
        results[10] = new int[] { 2, 3, 4 }; // 299:349
        results[11] = new int[] { 2, 3, 4 }; // 300:350
        results[12] = new int[] { 1, 2, 3 }; // 301:351
        results[13] = new int[] { 0, 1, 2 }; // 350:400
        results[14] = new int[] { 0 }; // 425:475

        //        // following results for `alternative map for descending` mentioned in header comment
        //        results[0] = new int[] { 15 }; // -25:25
        //        results[1] = new int[] { 12, 13 }; // 50:100
        //        results[2] = new int[] { 10, 11, 12 }; // 99:149
        //        results[3] = new int[] { 10, 11 }; // 100:150
        //        results[4] = new int[] { 9, 10, 11 }; // 101:151
        //        results[5] = new int[] { 7, 8, 9 }; // 150:200  // Actual: 2 partitions

        Long[] map = MAP_POINTS.clone();
        ArrayUtils.reverse(map);
        RangeMap rangeMap = getRangeMap(map);

        executeFieldRangeMultiPartitionTests(PARTITION_EDGE_CASES, rangeMap, BINARY_DESC_COMPARATOR_FACTORIES,
                BINARY_DESC_MAX_COMPARATOR_FACTORIES, RangePartitioningType.SPLIT, 16, results, 50);
    }

    @Test // (Stephen) Results Array Checked
    public void testFRMPCF_Split_ASC_D3_N16_EACH() throws HyracksDataException {
        int[][] results = new int[16][];
        results[0] = new int[] { 0 }; // 20:23
        results[1] = new int[] { 0 }; // 45:48  // Actual: 1
        results[2] = new int[] { 0 }; // 70:73
        results[3] = new int[] { 0 }; // 95:98
        results[4] = new int[] { 1 }; // 120:123
        results[5] = new int[] { 1 }; // 145:148
        results[6] = new int[] { 1 }; // 170:173
        results[7] = new int[] { 1 }; // 195:198
        results[8] = new int[] { 2 }; // 220:223
        results[9] = new int[] { 2 }; // 245:248
        results[10] = new int[] { 2 }; // 270:273
        results[11] = new int[] { 2 }; // 295:298
        results[12] = new int[] { 3 }; // 320:323
        results[13] = new int[] { 3 }; // 345:348
        results[14] = new int[] { 3 }; // 370:373
        results[15] = new int[] { 3 }; // 395:398

        RangeMap rangeMap = getRangeMap(MAP_POINTS);

        executeFieldRangeMultiPartitionTests(EACH_PARTITION, rangeMap, BINARY_ASC_COMPARATOR_FACTORIES,
                BINARY_ASC_MAX_COMPARATOR_FACTORIES, RangePartitioningType.SPLIT, 16, results, 3);
    }

    @Test // (Stephen) Results Array Checked
    public void testFRMPCF_Split_DESC_D3_N4_EACH() throws HyracksDataException {
        int[][] results = new int[16][];
        results[0] = new int[] { 3 }; // 20:23  // Actual: 0
        results[1] = new int[] { 3 }; // 45:48
        results[2] = new int[] { 3 }; // 70:73
        results[3] = new int[] { 3 }; // 95:98
        results[4] = new int[] { 2 }; // 120:123
        results[5] = new int[] { 2 }; // 145:148
        results[6] = new int[] { 2 }; // 170:173
        results[7] = new int[] { 2 }; // 195:198
        results[8] = new int[] { 1 }; // 220:223
        results[9] = new int[] { 1 }; // 245:248
        results[10] = new int[] { 1 }; // 270:273
        results[11] = new int[] { 1 }; // 295:298
        results[12] = new int[] { 0 }; // 320:323
        results[13] = new int[] { 0 }; // 345:348
        results[14] = new int[] { 0 }; // 370:373
        results[15] = new int[] { 0 }; // 395:398

        Long[] map = MAP_POINTS.clone();
        ArrayUtils.reverse(map);
        RangeMap rangeMap = getRangeMap(map);

        executeFieldRangeMultiPartitionTests(EACH_PARTITION, rangeMap, BINARY_ASC_COMPARATOR_FACTORIES,
                BINARY_ASC_MAX_COMPARATOR_FACTORIES, RangePartitioningType.SPLIT, 16, results, 3);
    }

    // ============================
    // deprecated SPLIT TESTS

    @Test // (Stephen) Results Array Checked
    public void testFieldRangeMultiPartitionAscSplit16Partitions() throws HyracksDataException {
        int[][] results = new int[15][];
        results[0] = new int[] { 0 };
        results[1] = new int[] { 2 };
        results[2] = new int[] { 3, 4 };
        results[3] = new int[] { 4 };
        results[4] = new int[] { 4 };
        results[5] = new int[] { 6 };
        results[6] = new int[] { 7, 8 };
        results[7] = new int[] { 8 };
        results[8] = new int[] { 8 };
        results[9] = new int[] { 10 };
        results[10] = new int[] { 11, 12 };
        results[11] = new int[] { 12 };
        results[12] = new int[] { 12 };
        results[13] = new int[] { 14 };
        results[14] = new int[] { 15 };

        RangeMap rangeMap = getRangeMap(MAP_POINTS);

        executeFieldRangeMultiPartitionTests(PARTITION_EDGE_CASES, rangeMap, BINARY_ASC_COMPARATOR_FACTORIES,
                BINARY_ASC_MAX_COMPARATOR_FACTORIES, RangePartitioningType.SPLIT, 16, results, 3);
    }
}

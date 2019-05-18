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

import org.apache.asterix.dataflow.data.nontagged.comparators.AGenericAscBinaryComparatorFactory;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
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
import org.apache.hyracks.dataflow.common.comm.io.FrameFixedFieldTupleAppender;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.data.marshalling.Integer64SerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.partition.range.RangeMap;
import org.apache.hyracks.dataflow.common.data.partition.range.StaticFieldRangeMultiPartitionComputerFactory;
import org.apache.hyracks.storage.common.arraylist.IntArrayList;
import org.apache.hyracks.test.support.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import junit.framework.TestCase;

public class FieldRangeMultiPartitionComputerFactoryTest extends TestCase {

    private final Integer64SerializerDeserializer int64Serde = Integer64SerializerDeserializer.INSTANCE;
    @SuppressWarnings("rawtypes")
    private final ISerializerDeserializer[] SerDers =
            new ISerializerDeserializer[] { Integer64SerializerDeserializer.INSTANCE };
    private final RecordDescriptor RecordDesc = new RecordDescriptor(SerDers);

    IBinaryComparatorFactory[] BINARY_ASC_COMPARATOR_FACTORIES = new IBinaryComparatorFactory[] {
            new AGenericAscBinaryComparatorFactory(BuiltinType.AINTERVAL, BuiltinType.AINTERVAL) };
    //    IBinaryRangeComparatorFactory[] BINARY_DESC_COMPARATOR_FACTORIES = new IBinaryRangeComparatorFactory[] {
    //            new PointableBinaryRangeDescComparatorFactory(LongPointable.FACTORY) };
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

    private final int FRAME_SIZE = 320;
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

    // The map of the partitions, listed as the split points.
    //partitions   {  0,   1,   2,   3,    4,    5,    6,    7,    8,    9,   10,   11,   12,   13,   14,   15,   16 };
    //map          { 0l, 25l, 50l, 75l, 100l, 125l, 150l, 175l, 200l, 225l, 250l, 275l, 300l, 325l, 350l, 375l, 400l };
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
            for (int i = 0; i < integers.length; ++i) {
                dos.write(ATypeTag.SERIALIZED_INT64_TYPE_TAG); // writes an interval (note for stephen ermshar)
                int64Serde.serialize(integers[i], dos);
                int64Serde.serialize(integers[i] + duration, dos);
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

    private ByteBuffer prepareData(IHyracksTaskContext ctx, Long[] integers, long duration)
            throws HyracksDataException {
        IFrame frame = new VSizeFrame(ctx);
        FrameFixedFieldTupleAppender fffta = new FrameFixedFieldTupleAppender(RecordDesc.getFieldCount());
        fffta.reset(frame, true);

        byte[] serializedIntegers = getIntervalBytes(integers, duration);
        for (int i = 0; i < integers.length; ++i) {
            fffta.appendField(serializedIntegers, i * INTEGER_LENGTH, INTEGER_LENGTH);
        }

        return frame.getBuffer();
    }

    private void executeFieldRangeMultiPartitionTests(Long[] integers, RangeMap rangeMap,
            IBinaryComparatorFactory[] minComparatorFactories, IBinaryComparatorFactory[] maxComparatorFactories,
            RangePartitioningType rangeType, int nParts, int[][] results, long duration) throws HyracksDataException {
        IHyracksTaskContext ctx = TestUtils.create(FRAME_SIZE);
        int[] rangeFields = new int[] { 0 };
        ITupleMultiPartitionComputerFactory frpcf = new StaticFieldRangeMultiPartitionComputerFactory(rangeFields,
                minComparatorFactories, maxComparatorFactories, rangeMap, rangeType);
        ITupleMultiPartitionComputer partitioner = frpcf.createPartitioner(ctx);
        partitioner.initialize();

        IFrameTupleAccessor accessor = new FrameTupleAccessor(RecordDesc);
        ByteBuffer buffer = prepareData(ctx, integers, duration);
        accessor.reset(buffer);

        IGrowableIntArray map = new IntArrayList(16, 1);

        for (int i = 0; i < results.length; ++i) {
            map.clear();
            partitioner.partition(accessor, i, nParts, map);
            checkPartitionResult(integers[i], results[i], map);
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

    private void checkPartitionResult(Long value, int[] results, IGrowableIntArray map) {
        if (results.length != map.size()) {
            Assert.assertEquals("The partition for value (" + value + ") gives different number of partitions",
                    results.length, map.size());
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
                Assert.assertEquals("Individual partitions for " + value + " do not match", getString(results),
                        getString(map));
                return;
            }
        }
    }

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

    //    @Test
    //    public void testFieldRangeMultiPartitionDescProject4AllPartitions() throws HyracksDataException {
    //        int[][] results = new int[16][];
    //        results[0] = new int[] { 3 };
    //        results[1] = new int[] { 3 };
    //        results[2] = new int[] { 3 };
    //        results[3] = new int[] { 3 };
    //        results[4] = new int[] { 2 };
    //        results[5] = new int[] { 2 };
    //        results[6] = new int[] { 2 };
    //        results[7] = new int[] { 2 };
    //        results[8] = new int[] { 1 };
    //        results[9] = new int[] { 1 };
    //        results[10] = new int[] { 1 };
    //        results[11] = new int[] { 1 };
    //        results[12] = new int[] { 0 };
    //        results[13] = new int[] { 0 };
    //        results[14] = new int[] { 0 };
    //        results[15] = new int[] { 0 };
    //
    //        Long[] map = MAP_POINTS.clone();
    //        ArrayUtils.reverse(map);
    //        RangeMap rangeMap = getRangeMap(map);
    //
    //        executeFieldRangeMultiPartitionTests(EACH_PARTITION, rangeMap, BINARY_DESC_COMPARATOR_FACTORIES,
    //                RangePartitioningType.PROJECT, 4, results);
    //    }
    //
    //    @Test
    //    public void testFieldRangeMultiPartitionAscProject16AllPartitions() throws HyracksDataException {
    //        int[][] results = new int[16][];
    //        results[0] = new int[] { 0 };
    //        results[1] = new int[] { 1 };
    //        results[2] = new int[] { 2 };
    //        results[3] = new int[] { 3 };
    //        results[4] = new int[] { 4 };
    //        results[5] = new int[] { 5 };
    //        results[6] = new int[] { 6 };
    //        results[7] = new int[] { 7 };
    //        results[8] = new int[] { 8 };
    //        results[9] = new int[] { 9 };
    //        results[10] = new int[] { 10 };
    //        results[11] = new int[] { 11 };
    //        results[12] = new int[] { 12 };
    //        results[13] = new int[] { 13 };
    //        results[14] = new int[] { 14 };
    //        results[15] = new int[] { 15 };
    //
    //        RangeMap rangeMap = getRangeMap(MAP_POINTS);
    //
    //        executeFieldRangeMultiPartitionTests(EACH_PARTITION, rangeMap, BINARY_ASC_COMPARATOR_FACTORIES,
    //                RangePartitioningType.PROJECT, 16, results);
    //    }
    //
    //    @Test
    //    public void testFieldRangeMultiPartitionDescProject16AllPartitions() throws HyracksDataException {
    //        int[][] results = new int[16][];
    //        results[0] = new int[] { 15 };
    //        results[1] = new int[] { 14 };
    //        results[2] = new int[] { 13 };
    //        results[3] = new int[] { 12 };
    //        results[4] = new int[] { 11 };
    //        results[5] = new int[] { 10 };
    //        results[6] = new int[] { 9 };
    //        results[7] = new int[] { 8 };
    //        results[8] = new int[] { 7 };
    //        results[9] = new int[] { 6 };
    //        results[10] = new int[] { 5 };
    //        results[11] = new int[] { 4 };
    //        results[12] = new int[] { 3 };
    //        results[13] = new int[] { 2 };
    //        results[14] = new int[] { 1 };
    //        results[15] = new int[] { 0 };
    //
    //        Long[] map = MAP_POINTS.clone();
    //        ArrayUtils.reverse(map);
    //        RangeMap rangeMap = getRangeMap(map);
    //
    //        executeFieldRangeMultiPartitionTests(EACH_PARTITION, rangeMap, BINARY_DESC_COMPARATOR_FACTORIES,
    //                RangePartitioningType.PROJECT, 16, results);
    //    }
    //
    //    @Test
    //    public void testFieldRangeMultiPartitionAscProject16Partitions() throws HyracksDataException {
    //        int[][] results = new int[15][];
    //        results[0] = new int[] { 0 };
    //        results[1] = new int[] { 2 };
    //        results[2] = new int[] { 3 };
    //        results[3] = new int[] { 4 };
    //        results[4] = new int[] { 4 };
    //        results[5] = new int[] { 6 };
    //        results[6] = new int[] { 7 };
    //        results[7] = new int[] { 8 };
    //        results[8] = new int[] { 8 };
    //        results[9] = new int[] { 10 };
    //        results[10] = new int[] { 11 };
    //        results[11] = new int[] { 12 };
    //        results[12] = new int[] { 12 };
    //        results[13] = new int[] { 14 };
    //        results[14] = new int[] { 15 };
    //
    //        RangeMap rangeMap = getRangeMap(MAP_POINTS);
    //
    //        executeFieldRangeMultiPartitionTests(PARTITION_EDGE_CASES, rangeMap, BINARY_ASC_COMPARATOR_FACTORIES,
    //                RangePartitioningType.PROJECT, 16, results);
    //    }
    //
    //    @Test
    //    public void testFieldRangeMultiPartitionAscProject4Partitions() throws HyracksDataException {
    //        int[][] results = new int[15][];
    //        results[0] = new int[] { 0 };
    //        results[1] = new int[] { 0 };
    //        results[2] = new int[] { 0 };
    //        results[3] = new int[] { 1 };
    //        results[4] = new int[] { 1 };
    //        results[5] = new int[] { 1 };
    //        results[6] = new int[] { 1 };
    //        results[7] = new int[] { 2 };
    //        results[8] = new int[] { 2 };
    //        results[9] = new int[] { 2 };
    //        results[10] = new int[] { 2 };
    //        results[11] = new int[] { 3 };
    //        results[12] = new int[] { 3 };
    //        results[13] = new int[] { 3 };
    //        results[14] = new int[] { 3 };
    //
    //        RangeMap rangeMap = getRangeMap(MAP_POINTS);
    //
    //        executeFieldRangeMultiPartitionTests(PARTITION_EDGE_CASES, rangeMap, BINARY_ASC_COMPARATOR_FACTORIES,
    //                RangePartitioningType.PROJECT, 4, results);
    //    }
    //
    //    @Test
    //    public void testFieldRangeMultiPartitionAscReplicate4Partitions() throws HyracksDataException {
    //        int[][] results = new int[15][];
    //        results[0] = new int[] { 0, 1, 2, 3 };
    //        results[1] = new int[] { 0, 1, 2, 3 };
    //        results[2] = new int[] { 0, 1, 2, 3 };
    //        results[3] = new int[] { 1, 2, 3 };
    //        results[4] = new int[] { 1, 2, 3 };
    //        results[5] = new int[] { 1, 2, 3 };
    //        results[6] = new int[] { 1, 2, 3 };
    //        results[7] = new int[] { 2, 3 };
    //        results[8] = new int[] { 2, 3 };
    //        results[9] = new int[] { 2, 3 };
    //        results[10] = new int[] { 2, 3 };
    //        results[11] = new int[] { 3 };
    //        results[12] = new int[] { 3 };
    //        results[13] = new int[] { 3 };
    //        results[14] = new int[] { 3 };
    //
    //        RangeMap rangeMap = getRangeMap(MAP_POINTS);
    //
    //        executeFieldRangeMultiPartitionTests(PARTITION_EDGE_CASES, rangeMap, BINARY_REPLICATE_COMPARATOR_FACTORIES,
    //                RangePartitioningType.REPLICATE, 4, results);
    //    }
    //
    //    @Test
    //    public void testFieldRangeMultiPartitionAscReplicate16Partitions() throws HyracksDataException {
    //        int[][] results = new int[15][];
    //        results[0] = new int[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };
    //        results[1] = new int[] { 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };
    //        results[2] = new int[] { 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };
    //        results[3] = new int[] { 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };
    //        results[4] = new int[] { 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };
    //        results[5] = new int[] { 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };
    //        results[6] = new int[] { 7, 8, 9, 10, 11, 12, 13, 14, 15 };
    //        results[7] = new int[] { 8, 9, 10, 11, 12, 13, 14, 15 };
    //        results[8] = new int[] { 8, 9, 10, 11, 12, 13, 14, 15 };
    //        results[9] = new int[] { 10, 11, 12, 13, 14, 15 };
    //        results[10] = new int[] { 11, 12, 13, 14, 15 };
    //        results[11] = new int[] { 12, 13, 14, 15 };
    //        results[12] = new int[] { 12, 13, 14, 15 };
    //        results[13] = new int[] { 14, 15 };
    //        results[14] = new int[] { 15 };
    //
    //        RangeMap rangeMap = getRangeMap(MAP_POINTS);
    //
    //        executeFieldRangeMultiPartitionTests(PARTITION_EDGE_CASES, rangeMap, BINARY_REPLICATE_COMPARATOR_FACTORIES,
    //                RangePartitioningType.REPLICATE, 16, results);
    //    }
}

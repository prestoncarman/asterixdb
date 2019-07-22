package org.apache.hyracks.tests.unit;

import junit.framework.TestCase;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITupleMultiPartitionComputer;
import org.apache.hyracks.api.dataflow.value.ITupleMultiPartitionComputerFactory;
import org.apache.hyracks.api.dataflow.value.RangePartitioningType;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.storage.IGrowableIntArray;
import org.apache.hyracks.data.std.accessors.LongBinaryComparatorFactory;
import org.apache.hyracks.dataflow.common.comm.io.FrameFixedFieldTupleAppender;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.data.marshalling.Integer64SerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.partition.range.RangeMap;
import org.apache.hyracks.dataflow.common.data.partition.range.StaticFieldRangeMultiPartitionComputerFactory;
import org.apache.hyracks.storage.common.arraylist.IntArrayList;
import org.apache.hyracks.test.support.TestUtils;
import org.junit.Assert;

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class AbstractFieldRangeMultiPartitionComputerFactoryTest extends TestCase {

    /*
     * These tests check the range partitioning types with various integer positions and range map split points.
     * The test for PROJECT also checks PROJECT_END and SPLIT, which have the same results for integers.
     *
     * The following points (X) will be tested for these 4 partitions.
     *
     *     X  -----------X----------XXX----------X----------XXX----------X------------XXX------------X------------  X
     *        -----------------------|-----------------------|-------------------------|--------------------------
     *
     * The following points (X) will be tested for these 16 partitions.
     *
     *     X  -----------X----------XXX----------X----------XXX----------X------------XXX------------X------------  X
     *        -----|-----|-----|-----|-----|-----|-----|-----|-----|-----|------|------|------|------|------|-----
     *
     * N4                0          )[           1          )[           2            )[             3
     * N16     0  )[  1 )[  2 )[  3 )[  4 )[  5 )[  6 )[  7 )[  8 )[  9 )[  10 )[  11 )[  12 )[  13 )[  14 )[  15
     * ASC   0     25    50    75    100   125   150   175   200   225   250    275    300    325    350    375    400
     *
     * First and last partitions include all values less than and greater than min and max split points respectively.
     */


    //result index {      0,   1,   2,   3,    4,    5,    6,    7,    8,    9,    10,   11,   12,   13,   14        };
    //points       {    -25l, 50l, 99l, 100l, 101l, 150l, 199l, 200l, 201l, 250l, 299l, 300l, 301l, 350l, 425l       };
    protected final Long[] PARTITION_EDGE_CASES =
            new Long[] { -25l, 50l, 99l, 100l, 101l, 150l, 199l, 200l, 201l, 250l, 299l, 300l, 301l, 350l, 425l };
    //map      {  25l, 50l, 75l, 100l, 125l, 150l, 175l, 200l, 225l, 250l, 275l, 300l, 325l, 350l, 375l };
    //splits   {    0,   1,   2,    3,    4,    5,    6,    7,    8,    9,   10,   11,   12,   13,   14 };
    protected final Long[] MAP_POINTS =
            new Long[] { 25l, 50l, 75l, 100l, 125l, 150l, 175l, 200l, 225l, 250l, 275l, 300l, 325l, 350l, 375l };
    private final Integer64SerializerDeserializer int64Serde = Integer64SerializerDeserializer.INSTANCE;
    @SuppressWarnings("rawtypes")
    private final ISerializerDeserializer[] SerDers =
            new ISerializerDeserializer[] { Integer64SerializerDeserializer.INSTANCE };
    private final RecordDescriptor RecordDesc = new RecordDescriptor(SerDers);
    private final int FRAME_SIZE = 320;
    private final int INTEGER_LENGTH = Long.BYTES;
    IBinaryComparatorFactory[] BINARY_ASC_COMPARATOR_FACTORIES =
            new IBinaryComparatorFactory[] { LongBinaryComparatorFactory.INSTANCE };

    private byte[] getIntegerBytes(Long[] integers) throws HyracksDataException {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutput dos = new DataOutputStream(bos);
            for (int i = 0; i < integers.length; ++i) {
                int64Serde.serialize(integers[i], dos);
            }
            bos.close();
            return bos.toByteArray();
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    protected RangeMap getRangeMap(Long[] integers) throws HyracksDataException {
        int offsets[] = new int[integers.length];
        for (int i = 0; i < integers.length; ++i) {
            offsets[i] = (i + 1) * INTEGER_LENGTH;
        }
        return new RangeMap(1, getIntegerBytes(integers), offsets);
    }

    private ByteBuffer prepareData(IHyracksTaskContext ctx, Long[] integers) throws HyracksDataException {
        IFrame frame = new VSizeFrame(ctx);
        FrameFixedFieldTupleAppender fffta = new FrameFixedFieldTupleAppender(RecordDesc.getFieldCount());
        fffta.reset(frame, true);

        byte[] serializedIntegers = getIntegerBytes(integers);
        for (int i = 0; i < integers.length; ++i) {
            fffta.appendField(serializedIntegers, i * INTEGER_LENGTH, INTEGER_LENGTH);
        }

        return frame.getBuffer();
    }

    protected void executeFieldRangeMultiPartitionTests(Long[] integers, RangeMap rangeMap,
            IBinaryComparatorFactory[] comparatorFactories, RangePartitioningType rangeType, int nParts,
            int[][] results) throws HyracksDataException {
        IHyracksTaskContext ctx = TestUtils.create(FRAME_SIZE);
        int[] rangeFields = new int[] { 0 };
        ITupleMultiPartitionComputerFactory frpcf = new StaticFieldRangeMultiPartitionComputerFactory(rangeFields,
                comparatorFactories, comparatorFactories, rangeMap, rangeType);
        ITupleMultiPartitionComputer partitioner = frpcf.createPartitioner(ctx);
        partitioner.initialize();

        IFrameTupleAccessor accessor = new FrameTupleAccessor(RecordDesc);
        ByteBuffer buffer = prepareData(ctx, integers);
        accessor.reset(buffer);

        IGrowableIntArray map = new IntArrayList(16, 1);

        System.out.println();
        for (int i = 0; i < results.length; i++) {
            map.clear();
            partitioner.partition(accessor, i, nParts, map);
            System.out.print("integer: (" + integers[i] + ")   ");
            System.out.println("E: " + Arrays.toString(results[i]) + "; A: " + getString(map));
        }

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
}

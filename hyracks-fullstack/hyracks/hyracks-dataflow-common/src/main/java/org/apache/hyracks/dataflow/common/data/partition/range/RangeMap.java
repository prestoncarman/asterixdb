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
package org.apache.hyracks.dataflow.common.data.partition.range;

import java.util.Arrays;
import org.apache.hyracks.api.dataflow.value.IRangeMap;

/**
 * <pre>
 * The range map stores the fields split values and their min and max values in a byte array.
 * The min value for each field followed by the first split value for each field followed by
 * the second split value for each field, etc. and ending with the max value for each field
 * For example:
 *                  min         split_point_idx0    split_point_idx1    split_point_idx2    split_point_idx3    max
 * in the byte[]:   f0,f1,f2    f0,f1,f2            f0,f1,f2            f0,f1,f2            f0,f1,f2            f0,f1,f2
 * fields would be = 3
 * we have 4 split points and the min and max, which gives us 5 partitions:
 *   |      p0      |      p1      |      p2      |      p3      |      p4      |
 *  min            sp0            sp1            sp2            sp3            max
 * endOffsets.length would be = 18 (min and max fields plus 12 for split points)
 * </pre>
 */
public class RangeMap implements IRangeMap {
    private static final long serialVersionUID = -7523433293419648234L;

    private final int fields;
    private final byte[] bytes;
    private final int[] endOffsets;

    public RangeMap(int numFields, byte[] bytes, int[] endOffsets) {
        this.fields = numFields;
        this.bytes = bytes;
        this.endOffsets = endOffsets;
    }

    /**
     * Divides the number of end offsets by the number of fields to get the number of
     * points (?) in the byte array; -2 so as to not count the min and max points.
     * @return the number of split points in this range map
     */
    @Override
    public int getSplitCount() {
        return endOffsets.length / fields - 2;
    }

    public byte[] getByteArray() {
        return bytes;
    }

    @Override
    public byte[] getByteArray(int fieldIndex, int splitIndex) {
        return bytes;
    }

    @Override
    public byte getTag(int fieldIndex, int splitIndex) {
        return getSplitValueTag(getSplitValueIndex(fieldIndex, splitIndex + 1));
    }

    @Override
    public int getStartOffset(int fieldIndex, int splitIndex) {
        return getSplitValueStart(getSplitValueIndex(fieldIndex, splitIndex + 1));
    }

    @Override
    public int getLength(int fieldIndex, int splitIndex) {
        return getSplitValueLength(getSplitValueIndex(fieldIndex, splitIndex + 1));
    }

    /** Translates fieldIndex & splitIndex into an index which is used to find information about that split value.
     * The combination of a fieldIndex & splitIndex uniquely identifies a split value of interest.
     * @param fieldIndex the field index within the splitIndex of interest (0 <= fieldIndex < fields)
     * @param splitIndex starts with 0,1,2,.. etc
     * @return the index of the desired split value that could be used with {@code bytes} & {@code endOffsets}.
     */
    private int getSplitValueIndex(int fieldIndex, int splitIndex) {
        return splitIndex * fields + fieldIndex;
    }

    /**
     * @param splitValueIndex is the combination of the split index + the field index within that split index
     * @return the type tag of a specific field in a specific split point
     */
    private byte getSplitValueTag(int splitValueIndex) {
        return bytes[getSplitValueStart(splitValueIndex)];
    }

    /**
     * @param splitValueIndex is the combination of the split index + the field index within that split index
     * @return the location of a split value in the byte array {@code bytes}
     */
    private int getSplitValueStart(int splitValueIndex) {
        int start = 0;
        if (splitValueIndex != 0) {
            start = endOffsets[splitValueIndex - 1];
        }
        return start;
    }

    /**
     * @param splitValueIndex is the combination of the split index + the field index within that split index
     * @return the length of a split value
     */
    private int getSplitValueLength(int splitValueIndex) {
        int length = endOffsets[splitValueIndex];
        if (splitValueIndex != 0) {
            length -= endOffsets[splitValueIndex - 1];
        }
        return length;
    }

    @Override
    public byte[] getMinByteArray(int columnIndex) {
        return bytes;
    }

    @Override
    public int getMinStartOffset(int columnIndex) {
        return getSplitValueStart(getSplitValueIndex(columnIndex, getMinIndex()));
    }

    @Override
    public int getMinLength(int columnIndex) {
        return getSplitValueLength(getSplitValueIndex(columnIndex, getMinIndex()));
    }

    @Override
    public byte getMinTag(int columnIndex) {
        return getSplitValueTag(getSplitValueIndex(columnIndex, getMinIndex()));
    }

    @Override
    public byte[] getMaxByteArray(int columnIndex) {
        return bytes;
    }

    @Override
    public int getMaxStartOffset(int columnIndex) {
        return getSplitValueStart(getSplitValueIndex(columnIndex, getMaxIndex()));
    }

    @Override
    public int getMaxLength(int columnIndex) {
        return getSplitValueLength(getSplitValueIndex(columnIndex, getMaxIndex()));
    }

    @Override
    public byte getMaxTag(int columnIndex) {
        return getSplitValueTag(getSplitValueIndex(columnIndex, getMaxIndex()));
    }

    private int getMaxIndex() {
        return endOffsets.length / fields - 1;
    }

    private int getMinIndex() {
        return 0;
    }

    @Override
    public int getMaxSlotFromPartition(int partition, int nPartitions) {
        double rangesPerPart = 1.0;
        if (getSplitCount() + 1 > nPartitions) {
            rangesPerPart = ((double) getSplitCount() + 1) / nPartitions;
        }
        return (int) Math.ceil((partition + 1) * rangesPerPart) - 1;
    }

    @Override
    public int getMinSlotFromPartition(int partition, int nPartitions) {
        double rangesPerPart = 1.0;
        if (getSplitCount() + 1 > nPartitions) {
            rangesPerPart = ((double) getSplitCount() + 1) / nPartitions;
        }
        return (int) Math.ceil(partition * rangesPerPart) - 1;
    }

    @Override
    public int getPartitionFromSlot(int slot, int nPartitions) {
        double rangesPerPart = 1.0;
        if (getSplitCount() + 1 > nPartitions) {
            rangesPerPart = ((double) getSplitCount() + 1) / nPartitions;
        }
        return (int) Math.floor(slot / rangesPerPart);
    }

    @Override
    public int hashCode() {
        return fields + Arrays.hashCode(bytes) + Arrays.hashCode(endOffsets);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof RangeMap)) {
            return false;
        }
        RangeMap other = (RangeMap) object;
        return fields == other.fields && Arrays.equals(endOffsets, other.endOffsets)
                && Arrays.equals(bytes, other.bytes);
    }
}

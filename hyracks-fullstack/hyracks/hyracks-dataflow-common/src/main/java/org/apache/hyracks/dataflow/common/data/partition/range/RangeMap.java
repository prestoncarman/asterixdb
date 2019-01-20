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

import org.apache.hyracks.api.dataflow.value.IRangeMap;

/**
 * <pre>
 * The range map stores the fields split values in a byte array.
 * The first split value for each field followed by the second split value for each field, etc. For example:
 *                  split_point_idx0    split_point_idx1    split_point_idx2    split_point_idx3    split_point_idx4
 * in the byte[]:   f0,f1,f2            f0,f1,f2            f0,f1,f2            f0,f1,f2            f0,f1,f2
 * numFields would be = 3
 * we have 5 split points, which gives us 6 partitions:
 *      p1  |       p2      |       p3      |       p4      |       p5      |       p6
 *          sp0             sp1             sp2             sp3             sp4
 * endOffsets.length would be = 15
 * </pre>
 */
public class RangeMap implements IRangeMap {
    private static final long serialVersionUID = 1L;
    private final int numFields;
    private final byte[] bytes;
    private final int[] endOffsets;

    public RangeMap(int numFields, byte[] bytes, int[] endOffsets) {
        this.numFields = numFields;
        this.bytes = bytes;
        this.endOffsets = endOffsets;
    }

    @Override
    public int getSplitCount() {
        return endOffsets.length / numFields - 2;
    }

    @Override
    public byte[] getByteArray(int columnIndex, int splitIndex) {
        return bytes;
    }

    @Override
    public byte getTag(int columnIndex, int splitIndex) {
        return getSplitValueTag(getFieldIndex(columnIndex, splitIndex + 1));
    }

    @Override
    public int getStartOffset(int columnIndex, int splitIndex) {
        return getFieldStart(getFieldIndex(columnIndex, splitIndex + 1));
    }

    @Override
    public int getLength(int columnIndex, int splitIndex) {
        return getFieldLength(getFieldIndex(columnIndex, splitIndex + 1));
    }

    private int getFieldIndex(int columnIndex, int splitIndex) {
        return columnIndex + splitIndex * numFields;
    }

    private byte getSplitValueTag(int index) {
        return bytes[getFieldStart(index)];
    }

    private int getFieldStart(int index) {
        int start = 0;
        if (index != 0) {
            start = endOffsets[index - 1];
        }
        return start;
    }

    private int getFieldLength(int index) {
        int length = endOffsets[index];
        if (index != 0) {
            length -= endOffsets[index - 1];
        }
        return length;
    }

    @Override
    public byte[] getMinByteArray(int columnIndex) {
        return bytes;
    }

    @Override
    public int getMinStartOffset(int columnIndex) {
        return getFieldStart(getFieldIndex(columnIndex, getMinIndex()));
    }

    @Override
    public int getMinLength(int columnIndex) {
        return getFieldLength(getFieldIndex(columnIndex, getMinIndex()));
    }

    @Override
    public byte getMinTag(int columnIndex) {
        return getSplitValueTag(getFieldIndex(columnIndex, getMinIndex()));
    }

    @Override
    public byte[] getMaxByteArray(int columnIndex) {
        return bytes;
    }

    @Override
    public int getMaxStartOffset(int columnIndex) {
        return getFieldStart(getFieldIndex(columnIndex, getMaxIndex()));
    }

    @Override
    public int getMaxLength(int columnIndex) {
        return getFieldLength(getFieldIndex(columnIndex, getMaxIndex()));
    }

    @Override
    public byte getMaxTag(int columnIndex) {
        return getSplitValueTag(getFieldIndex(columnIndex, getMaxIndex()));
    }

    private int getMaxIndex() {
        return endOffsets.length / numFields - 1;
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
}

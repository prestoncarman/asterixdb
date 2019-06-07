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

import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.IRangePartitionType.RangePartitioningType;
import org.apache.hyracks.api.dataflow.value.ITupleMultiPartitionComputer;
import org.apache.hyracks.api.dataflow.value.ITupleMultiPartitionComputerFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.storage.IGrowableIntArray;

public abstract class FieldRangeMultiPartitionComputerFactory implements ITupleMultiPartitionComputerFactory {
    private static final long serialVersionUID = 1L;
    private final int[] rangeFields;
    private IBinaryComparatorFactory[] minComparatorFactories;
    private IBinaryComparatorFactory[] maxComparatorFactories;
    private RangePartitioningType rangeType;

    public FieldRangeMultiPartitionComputerFactory(int[] rangeFields, IBinaryComparatorFactory[] minComparatorFactories,
            IBinaryComparatorFactory[] maxComparatorFactories, RangePartitioningType rangeType) {
        this.rangeFields = rangeFields;
        this.minComparatorFactories = minComparatorFactories;
        this.maxComparatorFactories = maxComparatorFactories;
        this.rangeType = rangeType;
    }

    protected abstract RangeMap getRangeMap(IHyracksTaskContext hyracksTaskContext) throws HyracksDataException;

    @Override
    public ITupleMultiPartitionComputer createPartitioner(IHyracksTaskContext hyracksTaskContext) {
        final IBinaryComparator[] minComparators = new IBinaryComparator[minComparatorFactories.length];
        for (int i = 0; i < minComparatorFactories.length; ++i) {
            minComparators[i] = minComparatorFactories[i].createBinaryComparator();
        }
        final IBinaryComparator[] maxComparators = new IBinaryComparator[maxComparatorFactories.length];
        for (int i = 0; i < maxComparatorFactories.length; ++i) {
            maxComparators[i] = maxComparatorFactories[i].createBinaryComparator();
        }

        return new ITupleMultiPartitionComputer() {
            private int partitionCount;
            private double rangesPerPart = 1;
            private int splitCount;
            private RangeMap rangeMap;

            @Override
            public void initialize() throws HyracksDataException {
                rangeMap = getRangeMap(hyracksTaskContext);
                splitCount = rangeMap.getSplitCount();
            }

            @Override
            public void partition(IFrameTupleAccessor accessor, int tIndex, int nParts, IGrowableIntArray map)
                    throws HyracksDataException {
                if (nParts == 1) {
                    map.add(0);
                    return;
                }
                // Map range partition to node partitions.
                if (partitionCount != nParts) {
                    partitionCount = nParts;
                    if (splitCount + 1 > nParts) {
                        rangesPerPart = ((double) splitCount + 1) / nParts;
                    }
                }
                getRangePartitions(accessor, tIndex, map);
            }

            /*
             * Determine the range partitions.
             */
            private void getRangePartitions(IFrameTupleAccessor accessor, int tIndex, IGrowableIntArray map)
                    throws HyracksDataException {
                int minPartition = getPartitionMap(binarySearchRangePartition(accessor, tIndex, minComparators));
                int maxPartition = getPartitionMap(binarySearchRangePartition(accessor, tIndex, maxComparators));
                switch (rangeType) {
                    case PROJECT:
                        addPartition(minPartition, map);
                        break;

                    case PROJECT_END:
                        addPartition(maxPartition, map);
                        break;

                    case REPLICATE:
                        for (int pid = minPartition; pid < partitionCount; ++pid) {
                            addPartition(pid, map);
                        }
                        break;

                    case SPLIT:
                        for (int pid = minPartition; pid <= maxPartition && pid < partitionCount; ++pid) {
                            addPartition(pid, map);
                        }
                        break;

                    default:
                }
            }

            private void addPartition(int partition, IGrowableIntArray map) {
                if (!hasPartition(partition, map)) {
                    map.add(partition);
                }
            }

            private int getPartitionMap(int partition) {
                return (int) Math.floor(partition / rangesPerPart);
            }

            private boolean hasPartition(int pid, IGrowableIntArray map) {
                for (int i = 0; i < map.size(); ++i) {
                    if (map.get(i) == pid) {
                        return true;
                    }
                }
                return false;
            }

            /*
             * Return first match or suggested index.
             */
            private int binarySearchRangePartition(IFrameTupleAccessor accessor, int tIndex,
                    IBinaryComparator[] comparators) throws HyracksDataException {
                int slotIndex = 0;
                for (int slotNumber = 0; slotNumber < rangeMap.getSplitCount(); ++slotNumber) {
                    int c = compareSlotAndFields(accessor, tIndex, slotIndex, comparators);
                    if (c < 0) {
                        return slotIndex;
                    }
                    slotIndex++;
                }
                return slotIndex;
            }

            private int compareSlotAndFields(IFrameTupleAccessor accessor, int tIndex, int slotNumber,
                    IBinaryComparator[] comparators) throws HyracksDataException {
                int c = 0;
                int startOffset = accessor.getTupleStartOffset(tIndex);
                int slotLength = accessor.getFieldSlotsLength();
                for (int fieldNum = 0; fieldNum < comparators.length; ++fieldNum) {
                    int fIdx = rangeFields[fieldNum];
                    int fStart = accessor.getFieldStartOffset(tIndex, fIdx);
                    int fEnd = accessor.getFieldEndOffset(tIndex, fIdx);
                    c = comparators[fieldNum].compare(accessor.getBuffer().array(), startOffset + slotLength + fStart,
                            fEnd - fStart, rangeMap.getByteArray(fieldNum, slotNumber),
                            rangeMap.getStartOffset(fieldNum, slotNumber), rangeMap.getLength(fieldNum, slotNumber));
                    if (c != 0) {
                        return c;
                    }
                }
                return c;
            }

        };
    }
}

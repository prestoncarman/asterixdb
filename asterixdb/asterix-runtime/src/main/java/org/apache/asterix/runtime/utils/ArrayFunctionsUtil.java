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
package org.apache.asterix.runtime.utils;

import java.util.List;

import org.apache.asterix.dataflow.data.nontagged.comparators.AObjectAscBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IValueReference;

public class ArrayFunctionsUtil {

    private static final IBinaryComparator COMP = AObjectAscBinaryComparatorFactory.INSTANCE.createBinaryComparator();

    private ArrayFunctionsUtil() {
    }

    public static <T extends IValueReference> T findItem(IValueReference item, List<T> sameHashes)
            throws HyracksDataException {
        T sameItem;
        for (int k = 0; k < sameHashes.size(); k++) {
            sameItem = sameHashes.get(k);
            if (COMP.compare(item.getByteArray(), item.getStartOffset(), item.getLength(), sameItem.getByteArray(),
                    sameItem.getStartOffset(), sameItem.getLength()) == 0) {
                return sameItem;
            }
        }
        return null;
    }
}
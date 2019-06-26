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

package org.apache.asterix.dataflow.data.nontagged.comparators;

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.asterix.dataflow.data.nontagged.serde.AIntervalSerializerDeserializer;
import org.apache.asterix.om.base.AInterval;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.junit.Assert;
import org.junit.Test;

import junit.framework.TestCase;

public class AIntervalPartialBinaryComparatorFactoryTest extends TestCase {

    /**
     * This test uses all the interval partial binary comparators to check combinations of cases where intervals' start
     * and end points are before, in between, after, and on the start and endpoints of the interval that they are being
     * compared to.
     */

    @SuppressWarnings("rawtypes")
    private final ISerializerDeserializer serde = AIntervalSerializerDeserializer.INSTANCE;

    /*
     * Compares all interval partial binary comparators.
     * 12 Cases include <, ==, > for start ASC, start DESC, end ASC, end DESC.
     */
    private final int INTERVAL_LENGTH = 1 + 2 * Long.BYTES;
    private final AInterval[] INTERVALS = new AInterval[] { new AInterval(5, 5, ATypeTag.SERIALIZED_DATETIME_TYPE_TAG),
            new AInterval(5, 10, ATypeTag.SERIALIZED_DATETIME_TYPE_TAG),
            new AInterval(5, 15, ATypeTag.SERIALIZED_DATETIME_TYPE_TAG),
            new AInterval(5, 20, ATypeTag.SERIALIZED_DATETIME_TYPE_TAG),
            new AInterval(5, 25, ATypeTag.SERIALIZED_DATETIME_TYPE_TAG),
            new AInterval(10, 10, ATypeTag.SERIALIZED_DATETIME_TYPE_TAG),
            new AInterval(10, 15, ATypeTag.SERIALIZED_DATETIME_TYPE_TAG),
            new AInterval(10, 20, ATypeTag.SERIALIZED_DATETIME_TYPE_TAG),
            new AInterval(10, 25, ATypeTag.SERIALIZED_DATETIME_TYPE_TAG),
            new AInterval(15, 15, ATypeTag.SERIALIZED_DATETIME_TYPE_TAG),
            new AInterval(15, 20, ATypeTag.SERIALIZED_DATETIME_TYPE_TAG),
            new AInterval(15, 25, ATypeTag.SERIALIZED_DATETIME_TYPE_TAG),
            new AInterval(20, 20, ATypeTag.SERIALIZED_DATETIME_TYPE_TAG),
            new AInterval(20, 25, ATypeTag.SERIALIZED_DATETIME_TYPE_TAG),
            new AInterval(25, 25, ATypeTag.SERIALIZED_DATETIME_TYPE_TAG),
            // BASE COMPARISON, compared to all preceding intervals
            new AInterval(10, 20, ATypeTag.SERIALIZED_DATETIME_TYPE_TAG) };

    IBinaryComparator START_ASC = AIntervalAscPartialBinaryComparatorFactory.INSTANCE.createBinaryComparator();
    IBinaryComparator END_ASC = AIntervalEndpointAscPartialBinaryComparatorFactory.INSTANCE.createBinaryComparator();
    IBinaryComparator START_DESC =
            AIntervalStartpointDescPartialBinaryComparatorFactory.INSTANCE.createBinaryComparator();
    IBinaryComparator END_DESC = AIntervalDescPartialBinaryComparatorFactory.INSTANCE.createBinaryComparator();

    @SuppressWarnings("unused")
    private byte[] getIntervalBytes() throws HyracksDataException {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutput dos = new DataOutputStream(bos);
            for (int i = 0; i < INTERVALS.length; ++i) {
                serde.serialize(INTERVALS[i], dos);
            }
            bos.close();
            return bos.toByteArray();
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    private void executeBinaryComparatorTests(IBinaryComparator bc, byte[] bytes, int[] results)
            throws HyracksDataException {
        // the last interval in INTERVALS is compared to all the preceding intervals
        for (int i = 0; i < results.length; ++i) {
            int leftOffset = i * INTERVAL_LENGTH; // get the ith interval
            int rightOffset = INTERVAL_LENGTH * (INTERVALS.length - 1); // get the last interval
            int c = bc.compare(bytes, leftOffset, INTERVAL_LENGTH, bytes, rightOffset, INTERVAL_LENGTH);
            Assert.assertEquals("results[" + i + "]", results[i], c);
        }
    }

    @Test
    public void testIntervalAsc() throws HyracksDataException {
        // Intervals with startpoints equal to the base are compared by endpoint.
        byte[] bytes = getIntervalBytes();
        int[] results = new int[] { -1, -1, -1, -1, -1, -1, -1, 0, 1, 1, 1, 1, 1, 1, 1 };
        executeBinaryComparatorTests(START_ASC, bytes, results);
    }

    @Test
    public void testIntervalEndpointAsc() throws HyracksDataException {
        // Intervals with endpoints equal to the base are compared by startpoint.
        byte[] bytes = getIntervalBytes();
        int[] results = new int[] { -1, -1, -1, -1, 1, -1, -1, 0, 1, -1, 1, 1, 1, 1, 1 };
        executeBinaryComparatorTests(END_ASC, bytes, results);
    }

    @Test
    public void testIntervalStartpointDesc() throws HyracksDataException {
        // Intervals with startpoints equal to the base are compared by endpoint.
        byte[] bytes = getIntervalBytes();
        // these results are the negative of the results in testIntervalAsc()
        int[] results = new int[] { 1, 1, 1, 1, 1, 1, 1, 0, -1, -1, -1, -1, -1, -1, -1 };
        executeBinaryComparatorTests(START_DESC, bytes, results);
    }

    @Test
    public void testIntervalDesc() throws HyracksDataException {
        // Intervals with endpoints equal to the base are compared by startpoint.
        byte[] bytes = getIntervalBytes();
        // these results are the negative of the results in testIntervalEndpointAsc()
        int[] results = new int[] { 1, 1, 1, 1, -1, 1, 1, 0, -1, 1, -1, -1, -1, -1, -1 };
        executeBinaryComparatorTests(END_DESC, bytes, results);
    }
}

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
package org.apache.asterix.dataflow.data.nontagged.serde;

import org.apache.asterix.om.base.AMutableInt64;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.partition.range.RangeMap;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;

public class RangeMapSerializerDeserializerTest {

    @Test
    public void test() throws HyracksDataException {

        ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
        DataOutput out = abvs.getDataOutput();

        AInt64SerializerDeserializer intSerde = AInt64SerializerDeserializer.INSTANCE;
        int[] offsets = new int[3];
        double[] percentages = new double[3];
        try {
            // Loop over list of integers
            for (int i = 0; i < 3; i++) {
                intSerde.serialize(new AMutableInt64((long) i + 10), out);
                offsets[i] = abvs.getLength();
                percentages[i] = i * 1.3;
            }
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
        RangeMap rangeMap = new RangeMap(1, abvs.getByteArray(), offsets, percentages);

        ArrayBackedValueStorage abvsResult = new ArrayBackedValueStorage();
        DataOutput outResult = abvsResult.getDataOutput();

        ARangeMapSerializerDeserializer rangeMapSerde = ARangeMapSerializerDeserializer.INSTANCE;
        rangeMapSerde.serialize(rangeMap, outResult);

        DataInput dataIn = new DataInputStream(new ByteArrayInputStream(abvsResult.getByteArray(),  0, abvsResult.getLength()));
        // check output
        RangeMap rangeMapResult = rangeMapSerde.deserialize(dataIn);
        Assert.assertEquals(rangeMapResult.getNumFields(), rangeMap.getNumFields());
        Assert.assertArrayEquals(rangeMapResult.getByteArray(), rangeMap.getByteArray());
        Assert.assertArrayEquals(rangeMapResult.getEndOffsets(), rangeMap.getEndOffsets());
        Assert.assertArrayEquals(rangeMapResult.getPercentages(), rangeMap.getPercentages(), 0.01);
        Assert.assertEquals(rangeMapResult, rangeMap);
    }
}

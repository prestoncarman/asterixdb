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

import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.marshalling.ByteArraySerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleArraySerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.IntArraySerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.partition.range.RangeMap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * This class serializes and de-serializes the binary data representation of an interval.
 *
 * Interval {
 *   byte type;
 *   T start;
 *   T end;
 * }
 *
 * T can be of type date, time or datetime.
 */
public class ARangeMapSerializerDeserializer implements ISerializerDeserializer<RangeMap> {

    private static final long serialVersionUID = 1L;

    public static final ARangeMapSerializerDeserializer INSTANCE = new ARangeMapSerializerDeserializer();

    private ARangeMapSerializerDeserializer() {
    }

    @Override
    public RangeMap deserialize(DataInput in) throws HyracksDataException {
        try {
            int numFields = IntegerSerializerDeserializer.read(in);
            byte[] bytes = ByteArraySerializerDeserializer.read(in);
            int[] endOffsets = IntArraySerializerDeserializer.read(in);
            double[] percentages = DoubleArraySerializerDeserializer.read(in);

            return new RangeMap(numFields, bytes, endOffsets, percentages);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public void serialize(RangeMap instance, DataOutput out) throws HyracksDataException {
        try {
            IntegerSerializerDeserializer.write(instance.getNumFields(), out);
            ByteArraySerializerDeserializer.write(instance.getByteArray(), out);
            IntArraySerializerDeserializer.write(instance.getEndOffsets(), out);
            DoubleArraySerializerDeserializer.write(instance.getPercentages(), out);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }
}
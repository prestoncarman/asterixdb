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
package org.apache.asterix.runtime.aggregates.std;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.asterix.dataflow.data.nontagged.serde.AIntervalSerializerDeserializer;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.ABinary;
import org.apache.asterix.om.base.AInterval;
import org.apache.asterix.om.base.AMutableBinary;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.runtime.exceptions.UnsupportedItemTypeException;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.ByteArraySerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleArraySerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.IntArraySerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;

public abstract class AbstractUnionIntervalRangeMapAggregateFunction extends AbstractAggregateFunction {

    private ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
    private IPointable inputVal = new VoidPointable();
    private final IScalarEvaluator eval;
    protected final IEvaluatorContext context;
    protected long currentMinStart;
    protected long currentMaxEnd;
    protected byte intervalType;

    private ISerializerDeserializer<ABinary> binarySerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ABINARY);
    private final AMutableBinary binary = new AMutableBinary(null, 0, 0);
    private final ArrayBackedValueStorage storage = new ArrayBackedValueStorage();
    private final int numOfPartitions;
    private final int numOrderByFields;
    private final int[] splitPoints;
    private final double[] percentages;

    public AbstractUnionIntervalRangeMapAggregateFunction(IScalarEvaluatorFactory[] args, IEvaluatorContext context,
            int numOfPartitions, int numOrderByFields, SourceLocation sourceLoc) throws HyracksDataException {
        super(sourceLoc);
        this.eval = args[0].createScalarEvaluator(context);
        this.context = context;
        this.numOfPartitions = numOfPartitions;
        this.numOrderByFields = numOrderByFields;
        this.splitPoints = new int[numOfPartitions - 1];
        this.percentages = new double[numOfPartitions - 1];
    }

    @Override
    public void init() throws HyracksDataException {
        // Initialize the resulting interval start and end positions
        currentMinStart = Long.MAX_VALUE;
        currentMaxEnd = Long.MIN_VALUE;
    }

    @Override
    public void step(IFrameTupleReference tuple) throws HyracksDataException {
        eval.evaluate(tuple, inputVal);
        byte[] data = inputVal.getByteArray();
        int offset = inputVal.getStartOffset();
        int len = inputVal.getLength();
        ATypeTag typeTag =
                EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(inputVal.getByteArray()[inputVal.getStartOffset()]);
        // Ignore SYSTEM_NULL.
        if (typeTag == ATypeTag.NULL || typeTag == ATypeTag.MISSING) {
            processNull();
        } else if (typeTag == ATypeTag.INTERVAL) {
            DataInput dataIn = new DataInputStream(new ByteArrayInputStream(data, offset + 1, len - 1));
            AInterval interval = AIntervalSerializerDeserializer.INSTANCE.deserialize(dataIn);
            currentMinStart = Math.min(currentMinStart, interval.getIntervalStart());
            currentMaxEnd = Math.max(currentMaxEnd, interval.getIntervalEnd());
            intervalType = (byte) interval.getIntervalType();
        }
    }

    @Override
    public void finish(IPointable result) throws HyracksDataException {

        resultStorage.reset();
        DataOutput allSplitValuesOut = storage.getDataOutput();
        int[] endOffsets = new int[splitPoints.length];
        try {

            double percentage = 1 / (double) percentages.length;
            long range = currentMaxEnd - currentMinStart;
            int nextSplitOffset = (int) range / numOfPartitions;
            int nextSplitIndex = nextSplitOffset - 1;

            for (int split = 0; split < splitPoints.length; split++) {
                splitPoints[split] = nextSplitIndex;
                percentages[split] = percentage;
                nextSplitIndex += nextSplitOffset;
            }

            for (int i = 0; i < splitPoints.length; i++) {
                allSplitValuesOut.write((byte) splitPoints[i]);
                endOffsets[i] = storage.getLength();
            }

        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
        serializeRangeMap(numOrderByFields, resultStorage.getByteArray(), endOffsets, result);
    }

    @Override
    public void finishPartial(IPointable result) throws HyracksDataException {
        if (isValidCoordinates(currentMinStart, currentMaxEnd)) {
            finish(result);
        }
    }

    protected void processNull() throws UnsupportedItemTypeException {
        throw new UnsupportedItemTypeException(sourceLoc, BuiltinFunctions.UNION_INTERVAL_RANGE,
                ATypeTag.SERIALIZED_SYSTEM_NULL_TYPE_TAG);
    }

    private boolean isValidCoordinates(double minX, double minY) {
        return (minX != Double.POSITIVE_INFINITY) && (minY != Double.POSITIVE_INFINITY);
    }

    private void serializeRangeMap(int numberFields, byte[] splitValues, int[] endOffsets, IPointable result)
            throws HyracksDataException {
        ArrayBackedValueStorage serRangeMap = new ArrayBackedValueStorage();
        IntegerSerializerDeserializer.write(numberFields, serRangeMap.getDataOutput());
        ByteArraySerializerDeserializer.write(splitValues, serRangeMap.getDataOutput());
        IntArraySerializerDeserializer.write(endOffsets, serRangeMap.getDataOutput());
        DoubleArraySerializerDeserializer.write(percentages, serRangeMap.getDataOutput());
        binary.setValue(serRangeMap.getByteArray(), serRangeMap.getStartOffset(), serRangeMap.getLength());
        storage.reset();
        binarySerde.serialize(binary, storage.getDataOutput());
        result.set(storage);
    }
}

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
package org.apache.asterix.runtime.evaluators.functions.temporal;

import static java.lang.Math.max;
import static java.lang.Math.min;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.asterix.common.annotations.MissingNullInOutFunction;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt64SerializerDeserializer;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.ABinary;
import org.apache.asterix.om.base.AMutableBinary;
import org.apache.asterix.om.base.AMutableInt32;
import org.apache.asterix.om.base.AMutableInterval;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.pointables.nonvisitor.AIntervalPointable;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.evaluators.functions.PointableHelper;
import org.apache.asterix.runtime.exceptions.TypeMismatchException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.TaggedValuePointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.ByteArraySerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleArraySerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.IntArraySerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;

@MissingNullInOutFunction
public class IntervalRangeMapDescriptor extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = IntervalRangeMapDescriptor::new;

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.INTERVAL_RANGE_MAP;
    }

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {

            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(final IEvaluatorContext ctx) throws HyracksDataException {

                return new IScalarEvaluator() {

                    private final ArrayBackedValueStorage storage = new ArrayBackedValueStorage();
                    private final DataOutput out = storage.getDataOutput();

                    private final TaggedValuePointable argPtr0 = TaggedValuePointable.FACTORY.createPointable();
                    private final TaggedValuePointable argPtr1 = TaggedValuePointable.FACTORY.createPointable();
                    private final TaggedValuePointable argPtr2 = TaggedValuePointable.FACTORY.createPointable();

                    private AIntervalPointable interval0 =
                            (AIntervalPointable) AIntervalPointable.FACTORY.createPointable();
                    private AIntervalPointable interval1 =
                            (AIntervalPointable) AIntervalPointable.FACTORY.createPointable();
                    private final IScalarEvaluator eval0 = args[0].createScalarEvaluator(ctx);
                    private final IScalarEvaluator eval1 = args[1].createScalarEvaluator(ctx);
                    private final IScalarEvaluator eval2 = args[2].createScalarEvaluator(ctx);

                    private final AMutableInterval aInterval = new AMutableInterval(0, 0, (byte) -1);
                    private final AMutableInt32 aInt32 = new AMutableInt32(0);
                    private final AMutableBinary binary = new AMutableBinary(null, 0, 0);
                    private ISerializerDeserializer<ABinary> binarySerde =
                            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ABINARY);

                    @Override
                    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
                        storage.reset();
                        eval0.evaluate(tuple, argPtr0);
                        eval1.evaluate(tuple, argPtr1);
                        eval2.evaluate(tuple, argPtr2);
                        if (PointableHelper.checkAndSetMissingOrNull(result, argPtr0, argPtr1, argPtr2)) {
                            return;
                        }
                        byte typeTag0 = argPtr0.getTag();
                        if (typeTag0 != ATypeTag.SERIALIZED_INTERVAL_TYPE_TAG) {
                            throw new TypeMismatchException(sourceLoc, getIdentifier(), 0, typeTag0,
                                    ATypeTag.SERIALIZED_INTERVAL_TYPE_TAG);
                        }
                        byte typeTag1 = argPtr1.getTag();
                        if (typeTag1 != ATypeTag.SERIALIZED_INTERVAL_TYPE_TAG) {
                            throw new TypeMismatchException(sourceLoc, getIdentifier(), 1, typeTag1,
                                    ATypeTag.SERIALIZED_INTERVAL_TYPE_TAG);
                        }
                        byte typeTag2 = argPtr2.getTag();
                        if (typeTag2 != ATypeTag.SERIALIZED_INT64_TYPE_TAG) {
                            throw new TypeMismatchException(sourceLoc, getIdentifier(), 2, typeTag2,
                                    ATypeTag.SERIALIZED_INT64_TYPE_TAG);
                        }
                        argPtr0.getValue(interval0);
                        argPtr1.getValue(interval1);
                        long currentMinStart = max(interval0.getStartValue(), interval1.getStartValue());
                        long currentMaxEnd = min(interval0.getEndValue(), interval1.getEndValue());
                        byte[] data = argPtr2.getByteArray();
                        int offset = argPtr2.getStartOffset();
                        int numOfPartitions = (int) AInt64SerializerDeserializer.getLong(data, offset + 1);
                        long[] splitPoints = new long[numOfPartitions - 1];
                        double[] percentages = new double[numOfPartitions - 1];
                        int numOrderByFields = 1;
                        DataOutput allSplitValuesOut = storage.getDataOutput();
                        int[] endOffsets = new int[splitPoints.length];
                        try {
                            double percentage = 1.0 / (double) percentages.length;
                            long range = currentMaxEnd - currentMinStart;
                            long nextSplitOffset = (int) range / numOfPartitions;
                            long nextSplitIndex = currentMinStart + nextSplitOffset - 1;

                            if (numOfPartitions < 2) {
                                splitPoints = new long[1];
                                percentages = new double[1];
                                endOffsets = new int[1];
                                percentage = 0.50;
                                nextSplitOffset = 1000;
                                nextSplitIndex = 999;
                            }
                            for (int split = 0; split < splitPoints.length; split++) {
                                splitPoints[split] = nextSplitIndex;
                                percentages[split] = percentage;
                                nextSplitIndex += nextSplitOffset;
                            }

                            for (int i = 0; i < splitPoints.length; i++) {
                                allSplitValuesOut.writeByte(interval0.getType());
                                if (interval0.getType() == ATypeTag.SERIALIZED_DATETIME_TYPE_TAG) {
                                    allSplitValuesOut.writeLong(splitPoints[i]);
                                } else {
                                    allSplitValuesOut.writeInt((int) splitPoints[i]);
                                }
                                endOffsets[i] = storage.getLength();
                            }

                        } catch (IOException e) {
                            throw HyracksDataException.create(e);
                        }
                        serializeRangeMap(numOrderByFields, storage.getByteArray(), endOffsets, result, percentages);

                    }

                    private void serializeRangeMap(int numberFields, byte[] splitValues, int[] endOffsets,
                            IPointable result, double[] percentages) throws HyracksDataException {
                        ArrayBackedValueStorage serRangeMap = new ArrayBackedValueStorage();
                        IntegerSerializerDeserializer.write(numberFields, serRangeMap.getDataOutput());
                        ByteArraySerializerDeserializer.write(splitValues, serRangeMap.getDataOutput());
                        IntArraySerializerDeserializer.write(endOffsets, serRangeMap.getDataOutput());
                        DoubleArraySerializerDeserializer.write(percentages, serRangeMap.getDataOutput());
                        binary.setValue(serRangeMap.getByteArray(), serRangeMap.getStartOffset(),
                                serRangeMap.getLength());
                        storage.reset();
                        binarySerde.serialize(binary, storage.getDataOutput());
                        result.set(storage);
                    }
                };
            }
        };
    }
}

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

import org.apache.asterix.common.annotations.MissingNullInOutFunction;
import org.apache.asterix.dataflow.data.nontagged.serde.ARangeMapSerializerDeserializer;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.IFunctionDescriptorFactory;
import org.apache.asterix.om.pointables.nonvisitor.AIntervalPointable;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import org.apache.asterix.runtime.evaluators.functions.PointableHelper;
import org.apache.asterix.runtime.exceptions.IncompatibleTypeException;
import org.apache.asterix.runtime.exceptions.TypeMismatchException;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.ByteArrayPointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.data.std.primitive.TaggedValuePointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import org.apache.hyracks.dataflow.common.data.partition.range.RangeMap;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;

@MissingNullInOutFunction
public class IntervalJoinRemoveDuplicatesDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;
    public static final IFunctionDescriptorFactory FACTORY = IntervalJoinRemoveDuplicatesDescriptor::new;

    @Override
    public FunctionIdentifier getIdentifier() {
        return BuiltinFunctions.INTERVAL_JOIN_REMOVE_DUPLICATES;
    }

    @Override
    public IScalarEvaluatorFactory createEvaluatorFactory(final IScalarEvaluatorFactory[] args) {
        return new IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IScalarEvaluator createScalarEvaluator(final IEvaluatorContext ctx) throws HyracksDataException {
                return new IScalarEvaluator() {

                    private int partition = ctx.getTaskContext().getTaskAttemptId().getTaskId().getPartition();
                    private ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
                    private DataOutput out = resultStorage.getDataOutput();
                    private TaggedValuePointable argPtr0 = TaggedValuePointable.FACTORY.createPointable();
                    private TaggedValuePointable argPtr1 = TaggedValuePointable.FACTORY.createPointable();
                    private IPointable argPtr2 = ByteArrayPointable.FACTORY.createPointable();
                    private AIntervalPointable interval0 =
                            (AIntervalPointable) AIntervalPointable.FACTORY.createPointable();
                    private AIntervalPointable interval1 =
                            (AIntervalPointable) AIntervalPointable.FACTORY.createPointable();
                    private IntegerPointable integerTuple0 = IntegerPointable.FACTORY.createPointable();
                    private IntegerPointable integerRangeMap = IntegerPointable.FACTORY.createPointable();
                    private IntegerPointable integerTuple1 = IntegerPointable.FACTORY.createPointable();
                    private LongPointable longTuple0 = LongPointable.FACTORY.createPointable();
                    private LongPointable longTuple1 = LongPointable.FACTORY.createPointable();
                    private LongPointable longRangeMap = LongPointable.FACTORY.createPointable();
                    private IScalarEvaluator eval0 = args[0].createScalarEvaluator(ctx);
                    private IScalarEvaluator eval1 = args[1].createScalarEvaluator(ctx);
                    private IScalarEvaluator eval2 = args[2].createScalarEvaluator(ctx);
                    private final ArrayBackedValueStorage s0 = new ArrayBackedValueStorage();
                    private final ArrayBackedValueStorage s1 = new ArrayBackedValueStorage();

                    private ISerializerDeserializer<ABoolean> booleanSerde =
                            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ABOOLEAN);

                    @Override
                    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
                        resultStorage.reset();
                        if (partition == 0) {
                            booleanSerde.serialize(ABoolean.TRUE, out);
                            result.set(resultStorage);
                            return;
                        }
                        eval0.evaluate(tuple, argPtr0);
                        eval1.evaluate(tuple, argPtr1);
                        eval2.evaluate(tuple, argPtr2);

                        if (PointableHelper.checkAndSetMissingOrNull(result, argPtr0, argPtr1, argPtr2)) {
                            return;
                        }

                        byte type0 = argPtr0.getTag();
                        byte type1 = argPtr1.getTag();

                        if (type0 == ATypeTag.SERIALIZED_INTERVAL_TYPE_TAG && type0 == type1) {
                            argPtr0.getValue(interval0);
                            argPtr1.getValue(interval1);
                            byte intervalType0 = interval0.getType();
                            byte intervalType1 = interval1.getType();

                            if (intervalType0 != intervalType1) {
                                throw new IncompatibleTypeException(sourceLoc, getIdentifier(), intervalType0,
                                        intervalType1);
                            }
                        } else if (type0 != ATypeTag.SERIALIZED_INTERVAL_TYPE_TAG) {
                            throw new TypeMismatchException(sourceLoc, getIdentifier(), 0, type0,
                                    ATypeTag.SERIALIZED_INTERVAL_TYPE_TAG);
                        } else {
                            throw new IncompatibleTypeException(sourceLoc, getIdentifier(), type0, type1);
                        }

                        ARangeMapSerializerDeserializer rangeMapSerde = ARangeMapSerializerDeserializer.INSTANCE;

                        // TODO Need feedback on how to appropriately access field data
                        ByteArrayInputStream inputBytes = new ByteArrayInputStream(argPtr2.getByteArray(),
                                argPtr2.getStartOffset() + 2, 81);
                        DataInput dataIn = new DataInputStream(inputBytes);
                        RangeMap rangeMapResult = rangeMapSerde.deserialize(dataIn);

                        ABoolean res = ABoolean.TRUE;

                        s0.reset();
                        s1.reset();
                        interval0.getTaggedStart(s0.getDataOutput());
                        interval1.getTaggedStart(s1.getDataOutput());

                        if (interval0.getTypeTag() == ATypeTag.DATE || interval0.getTypeTag() == ATypeTag.TIME) {
                            integerTuple0.set(s0.getByteArray(), s0.getStartOffset(), s0.getLength());
                            integerTuple1.set(s1.getByteArray(), s1.getStartOffset(), s1.getLength());
                            integerRangeMap.set(rangeMapResult.getByteArray(), rangeMapResult.getStartOffset(0, partition - 1),
                                    rangeMapResult.getLength(0, partition - 1));
                            if (integerTuple0.getInteger() < integerRangeMap.getInteger() && integerTuple1.getInteger() < integerRangeMap.getInteger()) {
                                res = ABoolean.FALSE;
                            }
                        } else if (interval0.getTypeTag() == ATypeTag.DATETIME) {
                            longTuple0.set(s0.getByteArray(), s0.getStartOffset(), s0.getLength());
                            longTuple1.set(s1.getByteArray(), s1.getStartOffset(), s1.getLength());
                            longRangeMap.set(rangeMapResult.getByteArray(), rangeMapResult.getStartOffset(0, partition - 1),
                                    rangeMapResult.getLength(0, partition - 1));
                            if (longTuple0.getLong() < longRangeMap.getLong() && longTuple1.getLong() < longRangeMap.getLong()) {
                                res = ABoolean.FALSE;
                            }
                        }

                        booleanSerde.serialize(res, out);
                        result.set(resultStorage);
                    }
                };
            }
        };
    }
}
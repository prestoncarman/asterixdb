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

@org.apache.asterix.common.annotations.MissingNullInOutFunction
public class UnionIntervalRangeDescriptor
        extends org.apache.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;
    public static final org.apache.asterix.om.functions.IFunctionDescriptorFactory FACTORY =
            UnionIntervalRangeDescriptor::new;

    @Override
    public org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory createEvaluatorFactory(
            final org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory[] args) {
        return new org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator createScalarEvaluator(
                    final org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext ctx)
                    throws org.apache.hyracks.api.exceptions.HyracksDataException {
                return new org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator() {

                    private final IntervalLogic il = new IntervalLogic();
                    private org.apache.hyracks.data.std.util.ArrayBackedValueStorage resultStorage =
                            new org.apache.hyracks.data.std.util.ArrayBackedValueStorage();
                    private java.io.DataOutput out = resultStorage.getDataOutput();
                    private org.apache.hyracks.data.std.primitive.TaggedValuePointable argPtr0 =
                            org.apache.hyracks.data.std.primitive.TaggedValuePointable.FACTORY.createPointable();
                    private org.apache.hyracks.data.std.primitive.TaggedValuePointable argPtr1 =
                            org.apache.hyracks.data.std.primitive.TaggedValuePointable.FACTORY.createPointable();
                    private org.apache.asterix.om.pointables.nonvisitor.AIntervalPointable interval0 =
                            (org.apache.asterix.om.pointables.nonvisitor.AIntervalPointable) org.apache.asterix.om.pointables.nonvisitor.AIntervalPointable.FACTORY
                                    .createPointable();
                    private org.apache.asterix.om.pointables.nonvisitor.AIntervalPointable interval1 =
                            (org.apache.asterix.om.pointables.nonvisitor.AIntervalPointable) org.apache.asterix.om.pointables.nonvisitor.AIntervalPointable.FACTORY
                                    .createPointable();
                    private org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator eval0 =
                            args[0].createScalarEvaluator(ctx);
                    private org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator eval1 =
                            args[1].createScalarEvaluator(ctx);

                    private final org.apache.asterix.om.base.AMutableInterval aInterval =
                            new org.apache.asterix.om.base.AMutableInterval(0, 0, (byte) -1);

                    @SuppressWarnings("unchecked")
                    private final org.apache.hyracks.api.dataflow.value.ISerializerDeserializer<org.apache.asterix.om.base.ANull> nullSerde =
                            org.apache.asterix.formats.nontagged.SerializerDeserializerProvider.INSTANCE
                                    .getSerializerDeserializer(org.apache.asterix.om.types.BuiltinType.ANULL);
                    @SuppressWarnings("unchecked")
                    private final org.apache.hyracks.api.dataflow.value.ISerializerDeserializer<org.apache.asterix.om.base.AInterval> intervalSerde =
                            org.apache.asterix.formats.nontagged.SerializerDeserializerProvider.INSTANCE
                                    .getSerializerDeserializer(org.apache.asterix.om.types.BuiltinType.AINTERVAL);

                    @Override
                    public void evaluate(org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference tuple,
                            org.apache.hyracks.data.std.api.IPointable result)
                            throws org.apache.hyracks.api.exceptions.HyracksDataException {
                        resultStorage.reset();
                        eval0.evaluate(tuple, argPtr0);
                        eval1.evaluate(tuple, argPtr1);

                        if (org.apache.asterix.runtime.evaluators.functions.PointableHelper
                                .checkAndSetMissingOrNull(result, argPtr0, argPtr1)) {
                            return;
                        }

                        byte type0 = argPtr0.getTag();
                        byte type1 = argPtr1.getTag();

                        if (type0 == org.apache.asterix.om.types.ATypeTag.SERIALIZED_INTERVAL_TYPE_TAG
                                && type0 == type1) {
                            argPtr0.getValue(interval0);
                            argPtr1.getValue(interval1);
                            byte intervalType0 = interval0.getType();
                            byte intervalType1 = interval1.getType();

                            if (intervalType0 != intervalType1) {
                                throw new org.apache.asterix.runtime.exceptions.IncompatibleTypeException(sourceLoc,
                                        getIdentifier(), intervalType0, intervalType1);
                            }

                            long start = Math.min(interval0.getStartValue(), interval1.getStartValue());
                            long end = Math.max(interval0.getEndValue(), interval1.getEndValue());
                            aInterval.setValue(start, end, intervalType0);
                            intervalSerde.serialize(aInterval, out);

                            result.set(resultStorage);
                        } else if (type0 != org.apache.asterix.om.types.ATypeTag.SERIALIZED_INTERVAL_TYPE_TAG) {
                            throw new org.apache.asterix.runtime.exceptions.TypeMismatchException(sourceLoc,
                                    getIdentifier(), 0, type0,
                                    org.apache.asterix.om.types.ATypeTag.SERIALIZED_INTERVAL_TYPE_TAG);
                        } else {
                            throw new org.apache.asterix.runtime.exceptions.IncompatibleTypeException(sourceLoc,
                                    getIdentifier(), type0, type1);
                        }
                    }

                };
            }
        };
    }

    @Override
    public org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier getIdentifier() {
        return org.apache.asterix.om.functions.BuiltinFunctions.GET_OVERLAPPING_INTERVAL;
    }
}

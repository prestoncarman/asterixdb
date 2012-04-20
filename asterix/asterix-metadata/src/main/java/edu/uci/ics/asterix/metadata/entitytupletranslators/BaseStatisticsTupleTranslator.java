/*
 * Copyright 2009-2010 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.metadata.entitytupletranslators;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Queue;

import edu.uci.ics.asterix.adm.parser.nontagged.AdmLexer;
import edu.uci.ics.asterix.adm.parser.nontagged.AdmLexerConstants;
import edu.uci.ics.asterix.adm.parser.nontagged.ParseException;
import edu.uci.ics.asterix.adm.parser.nontagged.Token;
import edu.uci.ics.asterix.builders.IAOrderedListBuilder;
import edu.uci.ics.asterix.builders.IARecordBuilder;
import edu.uci.ics.asterix.builders.IAUnorderedListBuilder;
import edu.uci.ics.asterix.builders.OrderedListBuilder;
import edu.uci.ics.asterix.builders.RecordBuilder;
import edu.uci.ics.asterix.builders.UnorderedListBuilder;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.dataflow.data.nontagged.printers.ARecordPrinterFactory;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ACircleSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ADateSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ADateTimeSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ADurationSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ALineSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.APoint3DSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.APointSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.APolygonSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ARectangleSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ATimeSerializerDeserializer;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.metadata.MetadataException;
import edu.uci.ics.asterix.metadata.bootstrap.MetadataRecordTypes;
import edu.uci.ics.asterix.metadata.declared.AqlSourceId;
import edu.uci.ics.asterix.metadata.statistics.BaseStatistics;
import edu.uci.ics.asterix.om.base.ABoolean;
import edu.uci.ics.asterix.om.base.ADouble;
import edu.uci.ics.asterix.om.base.AFloat;
import edu.uci.ics.asterix.om.base.AInt16;
import edu.uci.ics.asterix.om.base.AInt32;
import edu.uci.ics.asterix.om.base.AInt64;
import edu.uci.ics.asterix.om.base.AInt8;
import edu.uci.ics.asterix.om.base.AMutableDouble;
import edu.uci.ics.asterix.om.base.AMutableFloat;
import edu.uci.ics.asterix.om.base.AMutableInt16;
import edu.uci.ics.asterix.om.base.AMutableInt32;
import edu.uci.ics.asterix.om.base.AMutableInt64;
import edu.uci.ics.asterix.om.base.AMutableInt8;
import edu.uci.ics.asterix.om.base.AMutableString;
import edu.uci.ics.asterix.om.base.ANull;
import edu.uci.ics.asterix.om.base.ARecord;
import edu.uci.ics.asterix.om.base.AString;
import edu.uci.ics.asterix.om.types.AOrderedListType;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.AUnionType;
import edu.uci.ics.asterix.om.types.AUnorderedListType;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.util.NonTaggedFormatUtil;
import edu.uci.ics.hyracks.algebricks.core.algebra.data.IPrinterFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;

/**
 * @author rico
 * 
 */
public class BaseStatisticsTupleTranslator extends AbstractTupleTranslator<BaseStatistics> {
    private AdmLexer admLexer;
    private ArrayTupleBuilder tb = new ArrayTupleBuilder(4);
    private DataOutput dos = tb.getDataOutput();

    private int nullableFieldId = 0;

    private Queue<ArrayBackedValueStorage> baaosPool = new ArrayDeque<ArrayBackedValueStorage>();
    private Queue<IARecordBuilder> recordBuilderPool = new ArrayDeque<IARecordBuilder>();
    private Queue<IAOrderedListBuilder> orderedListBuilderPool = new ArrayDeque<IAOrderedListBuilder>();
    private Queue<IAUnorderedListBuilder> unorderedListBuilderPool = new ArrayDeque<IAUnorderedListBuilder>();

    private String mismatchErrorMessage = "Mismatch Type, expecting a value of type ";

    // Mutable Types..
    private AMutableInt8 aInt8 = new AMutableInt8((byte) 0);
    private AMutableInt16 aInt16 = new AMutableInt16((short) 0);
    private AMutableInt32 aInt32 = new AMutableInt32(0);
    private AMutableInt64 aInt64 = new AMutableInt64(0);
    private AMutableDouble aDouble = new AMutableDouble(0);
    private AMutableFloat aFloat = new AMutableFloat(0);
    private AMutableString aString = new AMutableString("");
    private AMutableString aStringFieldName = new AMutableString("");

    // Serializers
    @SuppressWarnings("unchecked")
    private ISerializerDeserializer<ADouble> doubleSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.ADOUBLE);
    @SuppressWarnings("unchecked")
    public ISerializerDeserializer<AString> stringSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.ASTRING);
    @SuppressWarnings("unchecked")
    private ISerializerDeserializer<AFloat> floatSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.AFLOAT);
    @SuppressWarnings("unchecked")
    private ISerializerDeserializer<AInt8> int8Serde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.AINT8);
    @SuppressWarnings("unchecked")
    private ISerializerDeserializer<AInt16> int16Serde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.AINT16);
    @SuppressWarnings("unchecked")
    private ISerializerDeserializer<AInt32> int32Serde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.AINT32);
    @SuppressWarnings("unchecked")
    private ISerializerDeserializer<AInt64> int64Serde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.AINT64);
    @SuppressWarnings("unchecked")
    private ISerializerDeserializer<ABoolean> booleanSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.ABOOLEAN);
    @SuppressWarnings("unchecked")
    private ISerializerDeserializer<ANull> nullSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.ANULL);
    // Field indexes of serialized BaseStatistics in a tuple.
    // First key field.
//    public static final int DATASET_DATAVERSENAME_TUPLE_FIELD_INDEX = 0;
//    // Second key field.
//    public static final int DATASET_DATASETNAME_TUPLE_FIELD_INDEX = 1;
    // Payload field containing serialized Dataset.
    public static final int DATASET_PAYLOAD_TUPLE_FIELD_INDEX = 0;
    @SuppressWarnings("unchecked")
    private ISerializerDeserializer<ARecord> recordSerDes = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(MetadataRecordTypes.BASE_STATISTICS_RECORDTYPE);

    private RecordDescriptor recordDescriptor = new RecordDescriptor(new ISerializerDeserializer[] { recordSerDes });

    private IPrinterFactory[] printFactories = new IPrinterFactory[] { new ARecordPrinterFactory(
            MetadataRecordTypes.BASE_STATISTICS_RECORDTYPE) };

    public BaseStatisticsTupleTranslator(boolean getTuple, int fieldCount) {
        super(getTuple, fieldCount);
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.uci.ics.asterix.metadata.api.IMetadataEntityTupleTranslator#
     * getMetadataEntytiFromTuple
     * (edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference)
     */
    @Override
    public BaseStatistics getMetadataEntytiFromTuple(ITupleReference tuple) throws MetadataException, IOException {

        byte[] serRecord = tuple.getFieldData(DATASET_PAYLOAD_TUPLE_FIELD_INDEX);
        int recordStartOffset = tuple.getFieldStart(DATASET_PAYLOAD_TUPLE_FIELD_INDEX);
        int recordLength = tuple.getFieldLength(DATASET_PAYLOAD_TUPLE_FIELD_INDEX);
        ByteArrayInputStream stream = new ByteArrayInputStream(serRecord, recordStartOffset, recordLength);
        DataInput in = new DataInputStream(stream);
        ARecord baseStatisticsRecord = (ARecord) recordSerDes.deserialize(in);
        String dataverseName = ((AString) baseStatisticsRecord
                .getValueByPos(MetadataRecordTypes.BASE_STATISTICS_ARECORD_DATAVERSE_INDEX)).getStringValue();
        String datasetName = ((AString) baseStatisticsRecord
                .getValueByPos(MetadataRecordTypes.BASE_STATISTICS_ARECORD_DATASET_INDEX)).getStringValue();
        AqlSourceId source = new AqlSourceId(dataverseName, datasetName);
        String nodeId = ((AString) baseStatisticsRecord
                .getValueByPos(MetadataRecordTypes.BASE_STATISTICS_ARECORD_NODEID_INDEX)).getStringValue();
        String sTupleCount = ((AString) baseStatisticsRecord
                .getValueByPos(MetadataRecordTypes.BASE_STATISTICS_ARECORD_TUPLECOUNT_INDEX)).getStringValue();
        BaseStatistics stats = new BaseStatistics(source);
        stats.setNodeId(nodeId);
        stats.setTupleCount(Long.parseLong(sTupleCount));
        return stats;
    }

    public void printTupleToPS(ITupleReference tuple, PrintStream ps) throws MetadataException, IOException {
        ps.append("{");
        byte[] serRecord = tuple.getFieldData(DATASET_PAYLOAD_TUPLE_FIELD_INDEX);
        int recordStartOffset = tuple.getFieldStart(DATASET_PAYLOAD_TUPLE_FIELD_INDEX);
        int recordLength = tuple.getFieldLength(DATASET_PAYLOAD_TUPLE_FIELD_INDEX);
        ByteArrayInputStream stream = new ByteArrayInputStream(serRecord, recordStartOffset, recordLength);
        DataInput in = new DataInputStream(stream);
        ARecord baseStatisticsRecord = (ARecord) recordSerDes.deserialize(in);
        ps.append("\"");
        ps.append(MetadataRecordTypes.BASE_STATISTICS_RECORDTYPE.getFieldNames()[MetadataRecordTypes.BASE_STATISTICS_ARECORD_DATAVERSE_INDEX]);
        ps.append("\"");
        ps.append(": ");
        ps.append("\"");
        ps.append(((AString) baseStatisticsRecord
                .getValueByPos(MetadataRecordTypes.BASE_STATISTICS_ARECORD_DATAVERSE_INDEX)).getStringValue());
        ps.append("\"");
        ps.append(", ");

        ps.append("\"");
        ps.append(MetadataRecordTypes.BASE_STATISTICS_RECORDTYPE.getFieldNames()[MetadataRecordTypes.BASE_STATISTICS_ARECORD_DATASET_INDEX]);
        ps.append("\"");
        ps.append(": ");
        ps.append("\"");
        ps.append(((AString) baseStatisticsRecord
                .getValueByPos(MetadataRecordTypes.BASE_STATISTICS_ARECORD_DATASET_INDEX)).getStringValue());
        ps.append("\"");
        ps.append(", ");

        ps.append("\"");
        ps.append(MetadataRecordTypes.BASE_STATISTICS_RECORDTYPE.getFieldNames()[MetadataRecordTypes.BASE_STATISTICS_ARECORD_NODEID_INDEX]);
        ps.append("\"");
        ps.append(": ");
        ps.append("\"");
        ps.append(((AString) baseStatisticsRecord
                .getValueByPos(MetadataRecordTypes.BASE_STATISTICS_ARECORD_NODEID_INDEX)).getStringValue());
        ps.append("\"");
        ps.append(", ");

        ps.append("\"");
        ps.append(MetadataRecordTypes.BASE_STATISTICS_RECORDTYPE.getFieldNames()[MetadataRecordTypes.BASE_STATISTICS_ARECORD_TUPLECOUNT_INDEX]);
        ps.append("\"");
        ps.append(": ");
        ps.append("\"");
        ps.append(((AString) baseStatisticsRecord
                .getValueByPos(MetadataRecordTypes.BASE_STATISTICS_ARECORD_TUPLECOUNT_INDEX)).getStringValue());
        ps.append("\"");
        MetadataRecordTypes.BASE_STATISTICS_RECORDTYPE.findFieldPosition("DataverseName");

        ps.append("}");
        ps.println();
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.uci.ics.asterix.metadata.api.IMetadataEntityTupleTranslator#
     * getTupleFromMetadataEntity(java.lang.Object)
     */
    @Override
    public ITupleReference getTupleFromMetadataEntity(BaseStatistics baseStatistics) throws IOException {
        // write the key in the first 2 fields of the tuple
        tupleBuilder.reset();
//        aString.setValue(baseStatistics.getAqlSourceId().getDataverseName());
//        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
//        tupleBuilder.addFieldEndOffset();
//        aString.setValue(baseStatistics.getAqlSourceId().getDatasetName());
//        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
//        tupleBuilder.addFieldEndOffset();

        // write the pay-load in the third field of the tuple

        recordBuilder.reset(MetadataRecordTypes.BASE_STATISTICS_RECORDTYPE);

        // write field 0
        fieldValue.reset();
        aString.setValue(baseStatistics.getAqlSourceId().getDataverseName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.BASE_STATISTICS_ARECORD_DATAVERSE_INDEX, fieldValue);

        // write field 1
        fieldValue.reset();
        aString.setValue(baseStatistics.getAqlSourceId().getDatasetName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.BASE_STATISTICS_ARECORD_DATASET_INDEX, fieldValue);

        // write field 2
        fieldValue.reset();
        aString.setValue(baseStatistics.getNodeId());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.BASE_STATISTICS_ARECORD_NODEID_INDEX, fieldValue);

        // write field 3
        fieldValue.reset();
        aString.setValue(String.valueOf(baseStatistics.getTupleCount()));
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.BASE_STATISTICS_ARECORD_TUPLECOUNT_INDEX, fieldValue);

        // write record
        recordBuilder.write(tupleBuilder.getDataOutput(), true);
        tupleBuilder.addFieldEndOffset();

        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
        return tuple;
    }

    public List<BaseStatistics> parseInputstream(InputStream in) throws AsterixException, IOException {
        admLexer = new AdmLexer(in);
        ARecordType recType = MetadataRecordTypes.BASE_STATISTICS_RECORDTYPE;
        int tupleNum = 0;
        List<BaseStatistics> liste = new ArrayList<BaseStatistics>();

        while (true) {
            ArrayTupleReference tuple = new ArrayTupleReference();
            tb.reset();
            if (!parseAdmInstance(recType, true, dos)) {
                break;
            }
            tb.addFieldEndOffset();

            tuple.reset(tb.getFieldEndOffsets(), tb.getByteArray());
            BaseStatistics statsObj = this.getMetadataEntytiFromTuple(tuple);
            liste.add(statsObj);

            tupleNum++;
        }
        return liste;
    }

    private boolean parseAdmInstance(IAType objectType, Boolean datasetRec, DataOutput out) throws AsterixException,
            IOException {
        Token token;
        try {
            token = admLexer.next();
        } catch (ParseException pe) {
            throw new AsterixException(pe);
        }
        if (token.kind == AdmLexerConstants.EOF) {
            return false;
        } else {
            admFromLexerStream(token, objectType, out, datasetRec);
            return true;
        }
    }

    private void admFromLexerStream(Token token, IAType objectType, DataOutput out, Boolean datasetRec)
            throws AsterixException, IOException {

        switch (token.kind) {
            case AdmLexerConstants.NULL_LITERAL: {
                if (checkType(ATypeTag.NULL, objectType, out)) {
                    nullSerde.serialize(ANull.NULL, out);
                } else
                    throw new AsterixException(" This field can not be null ");
                break;
            }
            case AdmLexerConstants.TRUE_LITERAL: {
                if (checkType(ATypeTag.BOOLEAN, objectType, out)) {
                    booleanSerde.serialize(ABoolean.TRUE, out);
                } else
                    throw new AsterixException(mismatchErrorMessage + objectType.getTypeName());
                break;
            }
            case AdmLexerConstants.BOOLEAN_CONS: {
                parseConstructor(ATypeTag.BOOLEAN, objectType, out);
                break;
            }
            case AdmLexerConstants.FALSE_LITERAL: {
                if (checkType(ATypeTag.BOOLEAN, objectType, out)) {
                    booleanSerde.serialize(ABoolean.FALSE, out);
                } else
                    throw new AsterixException(mismatchErrorMessage + objectType.getTypeName());
                break;
            }
            case AdmLexerConstants.DOUBLE_LITERAL: {
                if (checkType(ATypeTag.DOUBLE, objectType, out)) {
                    aDouble.setValue(Double.parseDouble(token.image));
                    doubleSerde.serialize(aDouble, out);
                } else
                    throw new AsterixException(mismatchErrorMessage + objectType.getTypeName());
                break;
            }
            case AdmLexerConstants.DOUBLE_CONS: {
                parseConstructor(ATypeTag.DOUBLE, objectType, out);
                break;
            }
            case AdmLexerConstants.FLOAT_LITERAL: {
                if (checkType(ATypeTag.FLOAT, objectType, out)) {
                    aFloat.setValue(Float.parseFloat(token.image));
                    floatSerde.serialize(aFloat, out);
                } else
                    throw new AsterixException(mismatchErrorMessage + objectType.getTypeName());
                break;
            }
            case AdmLexerConstants.FLOAT_CONS: {
                parseConstructor(ATypeTag.FLOAT, objectType, out);
                break;
            }
            case AdmLexerConstants.INT8_LITERAL: {
                if (checkType(ATypeTag.INT8, objectType, out)) {
                    parseInt8(token.image, out);
                } else
                    throw new AsterixException(mismatchErrorMessage + objectType.getTypeName());
                break;
            }
            case AdmLexerConstants.INT8_CONS: {
                parseConstructor(ATypeTag.INT8, objectType, out);
                break;
            }
            case AdmLexerConstants.INT16_LITERAL: {
                if (checkType(ATypeTag.INT16, objectType, out)) {
                    parseInt16(token.image, out);
                } else
                    throw new AsterixException(mismatchErrorMessage + objectType.getTypeName());
                break;
            }
            case AdmLexerConstants.INT16_CONS: {
                parseConstructor(ATypeTag.INT16, objectType, out);
                break;
            }
            case AdmLexerConstants.INT_LITERAL:
            case AdmLexerConstants.INT32_LITERAL: {
                if (checkType(ATypeTag.INT32, objectType, out)) {
                    parseInt32(token.image, out);
                } else
                    throw new AsterixException(mismatchErrorMessage + objectType.getTypeName());
                break;
            }
            case AdmLexerConstants.INT32_CONS: {
                parseConstructor(ATypeTag.INT32, objectType, out);
                break;
            }
            case AdmLexerConstants.INT64_LITERAL: {
                if (checkType(ATypeTag.INT64, objectType, out)) {
                    parseInt64(token.image, out);
                } else
                    throw new AsterixException(mismatchErrorMessage + objectType.getTypeName());
                break;
            }
            case AdmLexerConstants.INT64_CONS: {
                parseConstructor(ATypeTag.INT64, objectType, out);
                break;
            }
            case AdmLexerConstants.STRING_LITERAL: {
                if (checkType(ATypeTag.STRING, objectType, out)) {
                    aString.setValue(token.image.substring(1, token.image.length() - 1));
                    stringSerde.serialize(aString, out);
                } else
                    throw new AsterixException(mismatchErrorMessage + objectType.getTypeName());
                break;
            }
            case AdmLexerConstants.STRING_CONS: {
                parseConstructor(ATypeTag.STRING, objectType, out);
                break;
            }
            case AdmLexerConstants.DATE_CONS: {
                parseConstructor(ATypeTag.DATE, objectType, out);
                break;
            }
            case AdmLexerConstants.TIME_CONS: {
                parseConstructor(ATypeTag.TIME, objectType, out);
                break;
            }
            case AdmLexerConstants.DATETIME_CONS: {
                parseConstructor(ATypeTag.DATETIME, objectType, out);
                break;
            }
            case AdmLexerConstants.DURATION_CONS: {
                parseConstructor(ATypeTag.DURATION, objectType, out);
                break;
            }
            case AdmLexerConstants.POINT_CONS: {
                parseConstructor(ATypeTag.POINT, objectType, out);
                break;
            }
            case AdmLexerConstants.POINT3D_CONS: {
                parseConstructor(ATypeTag.POINT3D, objectType, out);
                break;
            }
            case AdmLexerConstants.CIRCLE_CONS: {
                parseConstructor(ATypeTag.CIRCLE, objectType, out);
                break;
            }
            case AdmLexerConstants.RECTANGLE_CONS: {
                parseConstructor(ATypeTag.RECTANGLE, objectType, out);
                break;
            }
            case AdmLexerConstants.LINE_CONS: {
                parseConstructor(ATypeTag.LINE, objectType, out);
                break;
            }
            case AdmLexerConstants.POLYGON_CONS: {
                parseConstructor(ATypeTag.POLYGON, objectType, out);
                break;
            }
            case AdmLexerConstants.START_UNORDERED_LIST: {
                if (checkType(ATypeTag.UNORDEREDLIST, objectType, out)) {
                    objectType = getComplexType(objectType, ATypeTag.UNORDEREDLIST);
                    parseUnorderedList((AUnorderedListType) objectType, out);
                } else
                    throw new AsterixException(mismatchErrorMessage + objectType.getTypeTag());
                break;
            }

            case AdmLexerConstants.START_ORDERED_LIST: {
                if (checkType(ATypeTag.ORDEREDLIST, objectType, out)) {
                    objectType = getComplexType(objectType, ATypeTag.ORDEREDLIST);
                    parseOrderedList((AOrderedListType) objectType, out);
                } else
                    throw new AsterixException(mismatchErrorMessage + objectType.getTypeTag());
                break;
            }
            case AdmLexerConstants.START_RECORD: {
                if (checkType(ATypeTag.RECORD, objectType, out)) {
                    objectType = getComplexType(objectType, ATypeTag.RECORD);
                    parseRecord((ARecordType) objectType, out, datasetRec);
                } else
                    throw new AsterixException(mismatchErrorMessage + objectType.getTypeTag());
                break;
            }
            case AdmLexerConstants.EOF: {
                break;
            }
            default: {
                throw new AsterixException("Unexpected ADM token kind: " + admLexer.tokenKindToString(token.kind) + ".");
            }
        }
    }

    private void parseConstructor(ATypeTag typeTag, IAType objectType, DataOutput out) throws AsterixException {
        try {
            Token token = admLexer.next();
            if (token.kind == AdmLexerConstants.CONSTRUCTOR_OPEN) {
                if (checkType(typeTag, objectType, out)) {
                    token = admLexer.next();
                    if (token.kind == AdmLexerConstants.STRING_LITERAL) {
                        switch (typeTag) {
                            case BOOLEAN:
                                parseBoolean(token.image.substring(1, token.image.length() - 1), out);
                                break;
                            case INT8:
                                parseInt8(token.image.substring(1, token.image.length() - 1), out);
                                break;
                            case INT16:
                                parseInt16(token.image.substring(1, token.image.length() - 1), out);
                                break;
                            case INT32:
                                parseInt32(token.image.substring(1, token.image.length() - 1), out);
                                break;
                            case INT64:
                                parseInt64(token.image.substring(1, token.image.length() - 1), out);
                                break;
                            case FLOAT:
                                aFloat.setValue(Float.parseFloat(token.image.substring(1, token.image.length() - 1)));
                                floatSerde.serialize(aFloat, out);
                                break;
                            case DOUBLE:
                                aDouble.setValue(Double.parseDouble(token.image.substring(1, token.image.length() - 1)));
                                doubleSerde.serialize(aDouble, out);
                                break;
                            case STRING:
                                aString.setValue(token.image.substring(1, token.image.length() - 1));
                                stringSerde.serialize(aString, out);
                                break;
                            case TIME:
                                parseTime(token.image.substring(1, token.image.length() - 1), out);
                                break;
                            case DATE:
                                parseDate(token.image.substring(1, token.image.length() - 1), out);
                                break;
                            case DATETIME:
                                parseDatetime(token.image.substring(1, token.image.length() - 1), out);
                                break;
                            case DURATION:
                                parseDuration(token.image.substring(1, token.image.length() - 1), out);
                                break;
                            case POINT:
                                parsePoint(token.image.substring(1, token.image.length() - 1), out);
                                break;
                            case POINT3D:
                                parsePoint3d(token.image.substring(1, token.image.length() - 1), out);
                                break;
                            case CIRCLE:
                                parseCircle(token.image.substring(1, token.image.length() - 1), out);
                                break;
                            case RECTANGLE:
                                parseRectangle(token.image.substring(1, token.image.length() - 1), out);
                                break;
                            case LINE:
                                parseLine(token.image.substring(1, token.image.length() - 1), out);
                                break;
                            case POLYGON:
                                parsePolygon(token.image.substring(1, token.image.length() - 1), out);
                                break;

                        }
                        token = admLexer.next();
                        if (token.kind == AdmLexerConstants.CONSTRUCTOR_CLOSE)
                            return;
                    }
                }
            }
        } catch (Exception e) {
            throw new AsterixException(e);
        }
        throw new AsterixException(mismatchErrorMessage + objectType.getTypeName());
    }

    private void parseBoolean(String bool, DataOutput out) throws AsterixException {
        String errorMessage = "This can not be an instance of boolean";
        try {
            if (bool.equals("true"))
                booleanSerde.serialize(ABoolean.TRUE, out);
            else if (bool.equals("false"))
                booleanSerde.serialize(ABoolean.FALSE, out);
            else
                throw new AsterixException(errorMessage);
        } catch (HyracksDataException e) {
            throw new AsterixException(errorMessage);
        }
    }

    private void parseInt8(String int8, DataOutput out) throws AsterixException {
        String errorMessage = "This can not be an instance of int8";
        try {
            boolean positive = true;
            byte value = 0;
            int offset = 0;

            if (int8.charAt(offset) == '+')
                offset++;
            else if (int8.charAt(offset) == '-') {
                offset++;
                positive = false;
            }
            for (; offset < int8.length(); offset++) {
                if (int8.charAt(offset) >= '0' && int8.charAt(offset) <= '9')
                    value = (byte) (value * 10 + int8.charAt(offset) - '0');
                else if (int8.charAt(offset) == 'i' && int8.charAt(offset + 1) == '8' && offset + 2 == int8.length())
                    break;
                else
                    throw new AsterixException(errorMessage);
            }
            if (value < 0)
                throw new AsterixException(errorMessage);
            if (value > 0 && !positive)
                value *= -1;
            aInt8.setValue(value);
            int8Serde.serialize(aInt8, out);
        } catch (HyracksDataException e) {
            throw new AsterixException(errorMessage);
        }
    }

    private void parseInt16(String int16, DataOutput out) throws AsterixException {
        String errorMessage = "This can not be an instance of int16";
        try {
            boolean positive = true;
            short value = 0;
            int offset = 0;

            if (int16.charAt(offset) == '+')
                offset++;
            else if (int16.charAt(offset) == '-') {
                offset++;
                positive = false;
            }
            for (; offset < int16.length(); offset++) {
                if (int16.charAt(offset) >= '0' && int16.charAt(offset) <= '9')
                    value = (short) (value * 10 + int16.charAt(offset) - '0');
                else if (int16.charAt(offset) == 'i' && int16.charAt(offset + 1) == '1'
                        && int16.charAt(offset + 2) == '6' && offset + 3 == int16.length())
                    break;
                else
                    throw new AsterixException(errorMessage);
            }
            if (value < 0)
                throw new AsterixException(errorMessage);
            if (value > 0 && !positive)
                value *= -1;
            aInt16.setValue(value);
            int16Serde.serialize(aInt16, out);
        } catch (HyracksDataException e) {
            throw new AsterixException(errorMessage);
        }
    }

    private void parseInt32(String int32, DataOutput out) throws AsterixException {

        String errorMessage = "This can not be an instance of int32";
        try {
            boolean positive = true;
            int value = 0;
            int offset = 0;

            if (int32.charAt(offset) == '+')
                offset++;
            else if (int32.charAt(offset) == '-') {
                offset++;
                positive = false;
            }
            for (; offset < int32.length(); offset++) {
                if (int32.charAt(offset) >= '0' && int32.charAt(offset) <= '9')
                    value = (value * 10 + int32.charAt(offset) - '0');
                else if (int32.charAt(offset) == 'i' && int32.charAt(offset + 1) == '3'
                        && int32.charAt(offset + 2) == '2' && offset + 3 == int32.length())
                    break;
                else
                    throw new AsterixException(errorMessage);
            }
            if (value < 0)
                throw new AsterixException(errorMessage);
            if (value > 0 && !positive)
                value *= -1;

            aInt32.setValue(value);
            int32Serde.serialize(aInt32, out);
        } catch (HyracksDataException e) {
            throw new AsterixException(errorMessage);
        }
    }

    private void parseInt64(String int64, DataOutput out) throws AsterixException {
        String errorMessage = "This can not be an instance of int64";
        try {
            boolean positive = true;
            long value = 0;
            int offset = 0;

            if (int64.charAt(offset) == '+')
                offset++;
            else if (int64.charAt(offset) == '-') {
                offset++;
                positive = false;
            }
            for (; offset < int64.length(); offset++) {
                if (int64.charAt(offset) >= '0' && int64.charAt(offset) <= '9')
                    value = (value * 10 + int64.charAt(offset) - '0');
                else if (int64.charAt(offset) == 'i' && int64.charAt(offset + 1) == '6'
                        && int64.charAt(offset + 2) == '4' && offset + 3 == int64.length())
                    break;
                else
                    throw new AsterixException(errorMessage);
            }
            if (value < 0)
                throw new AsterixException(errorMessage);
            if (value > 0 && !positive)
                value *= -1;

            aInt64.setValue(value);
            int64Serde.serialize(aInt64, out);
        } catch (HyracksDataException e) {
            throw new AsterixException(errorMessage);
        }
    }

    private void parsePoint(String point, DataOutput out) throws AsterixException {
        try {
            APointSerializerDeserializer.parse(point, out);
        } catch (HyracksDataException e) {
            throw new AsterixException(e);
        }
    }

    private void parsePoint3d(String point3d, DataOutput out) throws AsterixException {
        try {
            APoint3DSerializerDeserializer.parse(point3d, out);
        } catch (HyracksDataException e) {
            throw new AsterixException(e);
        }
    }

    private void parseCircle(String circle, DataOutput out) throws AsterixException {
        try {
            ACircleSerializerDeserializer.parse(circle, out);
        } catch (HyracksDataException e) {
            throw new AsterixException(e);
        }
    }

    private void parseRectangle(String rectangle, DataOutput out) throws AsterixException {
        try {
            ARectangleSerializerDeserializer.parse(rectangle, out);
        } catch (HyracksDataException e) {
            throw new AsterixException(e);
        }
    }

    private void parseLine(String line, DataOutput out) throws AsterixException {
        try {
            ALineSerializerDeserializer.parse(line, out);
        } catch (HyracksDataException e) {
            throw new AsterixException(e);
        }
    }

    private void parsePolygon(String polygon, DataOutput out) throws AsterixException, IOException {
        try {
            APolygonSerializerDeserializer.parse(polygon, out);
        } catch (HyracksDataException e) {
            throw new AsterixException(e);
        }
    }

    private void parseTime(String time, DataOutput out) throws AsterixException {
        try {
            ATimeSerializerDeserializer.parse(time, out);
        } catch (HyracksDataException e) {
            throw new AsterixException(e);
        }
    }

    private void parseDate(String date, DataOutput out) throws AsterixException, IOException {
        try {
            ADateSerializerDeserializer.parse(date, out);
        } catch (HyracksDataException e) {
            throw new AsterixException(e);
        }
    }

    private void parseDatetime(String datetime, DataOutput out) throws AsterixException, IOException {
        try {
            ADateTimeSerializerDeserializer.parse(datetime, out);
        } catch (HyracksDataException e) {
            throw new AsterixException(e);
        }
    }

    private void parseDuration(String duration, DataOutput out) throws AsterixException {
        try {
            ADurationSerializerDeserializer.parse(duration, out);
        } catch (HyracksDataException e) {
            throw new AsterixException(e);
        }

    }

    private IAType getComplexType(IAType aObjectType, ATypeTag tag) {

        if (aObjectType == null) {
            return null;
        }

        if (aObjectType.getTypeTag() == tag)
            return aObjectType;

        if (aObjectType.getTypeTag() == ATypeTag.UNION) {
            unionList = ((AUnionType) aObjectType).getUnionList();
            for (int i = 0; i < unionList.size(); i++)
                if (unionList.get(i).getTypeTag() == tag) {
                    return unionList.get(i);
                }
        }
        return null; // wont get here
    }

    List<IAType> unionList;

    private boolean checkType(ATypeTag expectedTypeTag, IAType aObjectType, DataOutput out) throws IOException {

        if (aObjectType == null)
            return true;

        if (aObjectType.getTypeTag() != ATypeTag.UNION) {
            if (expectedTypeTag == aObjectType.getTypeTag())
                return true;
        } else { // union
            unionList = ((AUnionType) aObjectType).getUnionList();
            for (int i = 0; i < unionList.size(); i++)
                if (unionList.get(i).getTypeTag() == expectedTypeTag)
                    return true;
        }
        return false;
    }

    private void parseRecord(ARecordType recType, DataOutput out, Boolean datasetRec) throws IOException,
            AsterixException {

        ArrayBackedValueStorage fieldValueBuffer = getTempBuffer();
        ArrayBackedValueStorage fieldNameBuffer = getTempBuffer();
        IARecordBuilder recBuilder = getRecordBuilder();

        // Boolean[] nulls = null;
        BitSet nulls = null;
        if (datasetRec) {
            if (recType != null) {
                nulls = new BitSet(recType.getFieldNames().length);
                recBuilder.reset(recType);
            } else
                recBuilder.reset(null);
        } else if (recType != null) {
            nulls = new BitSet(recType.getFieldNames().length);
            recBuilder.reset(recType);
        } else
            recBuilder.reset(null);

        recBuilder.init();
        Token token = null;
        boolean inRecord = true;
        boolean expectingRecordField = false;
        boolean first = true;

        Boolean openRecordField = false;
        int fieldId = 0;
        IAType fieldType = null;
        do {
            token = nextToken();
            switch (token.kind) {
                case AdmLexerConstants.END_RECORD: {
                    if (expectingRecordField) {
                        throw new AsterixException("Found END_RECORD while expecting a record field.");
                    }
                    inRecord = false;
                    break;
                }
                case AdmLexerConstants.STRING_LITERAL: {
                    // we've read the name of the field
                    // now read the content
                    fieldNameBuffer.reset();
                    fieldValueBuffer.reset();
                    expectingRecordField = false;

                    if (recType != null) {
                        String fldName = token.image.substring(1, token.image.length() - 1);
                        fieldId = recBuilder.getFieldId(fldName);
                        if (fieldId < 0 && !recType.isOpen()) {
                            throw new AsterixException("This record is closed, you can not add extra fields !!");
                        } else if (fieldId < 0 && recType.isOpen()) {
                            aStringFieldName.setValue(token.image.substring(1, token.image.length() - 1));
                            stringSerde.serialize(aStringFieldName, fieldNameBuffer.getDataOutput());
                            openRecordField = true;
                            fieldType = null;
                        } else {
                            // a closed field
                            nulls.set(fieldId);
                            fieldType = recType.getFieldTypes()[fieldId];
                            openRecordField = false;
                        }
                    } else {
                        aStringFieldName.setValue(token.image.substring(1, token.image.length() - 1));
                        stringSerde.serialize(aStringFieldName, fieldNameBuffer.getDataOutput());
                        openRecordField = true;
                        fieldType = null;
                    }

                    token = nextToken();
                    if (token.kind != AdmLexerConstants.COLON) {
                        throw new AsterixException("Unexpected ADM token kind: "
                                + admLexer.tokenKindToString(token.kind) + " while expecting \":\".");
                    }

                    token = nextToken();
                    this.admFromLexerStream(token, fieldType, fieldValueBuffer.getDataOutput(), false);
                    if (openRecordField) {
                        if (fieldValueBuffer.getBytes()[0] != ATypeTag.NULL.serialize())
                            recBuilder.addField(fieldNameBuffer, fieldValueBuffer);
                    } else if (recType.getFieldTypes()[fieldId].getTypeTag() == ATypeTag.UNION) {
                        if (NonTaggedFormatUtil.isOptionalField((AUnionType) recType.getFieldTypes()[fieldId])) {
                            if (fieldValueBuffer.getBytes()[0] != ATypeTag.NULL.serialize()) {
                                recBuilder.addField(fieldId, fieldValueBuffer);
                            }
                        }
                    } else {
                        recBuilder.addField(fieldId, fieldValueBuffer);
                    }

                    break;
                }
                case AdmLexerConstants.COMMA: {
                    if (first) {
                        throw new AsterixException("Found COMMA before any record field.");
                    }
                    if (expectingRecordField) {
                        throw new AsterixException("Found COMMA while expecting a record field.");
                    }
                    expectingRecordField = true;
                    break;
                }
                default: {
                    throw new AsterixException("Unexpected ADM token kind: " + admLexer.tokenKindToString(token.kind)
                            + " while parsing record fields.");
                }
            }
            first = false;
        } while (inRecord);

        if (recType != null) {
            nullableFieldId = checkNullConstraints(recType, nulls);
            if (nullableFieldId != -1)
                throw new AsterixException("Field " + nullableFieldId + " can not be null");
        }
        recBuilder.write(out, true);
        returnRecordBuilder(recBuilder);
        returnTempBuffer(fieldNameBuffer);
        returnTempBuffer(fieldValueBuffer);
    }

    private int checkNullConstraints(ARecordType recType, BitSet nulls) {

        boolean isNull = false;
        for (int i = 0; i < recType.getFieldTypes().length; i++)
            if (nulls.get(i) == false) {
                IAType type = recType.getFieldTypes()[i];
                if (type.getTypeTag() != ATypeTag.NULL && type.getTypeTag() != ATypeTag.UNION)
                    return i;

                if (type.getTypeTag() == ATypeTag.UNION) { // union
                    unionList = ((AUnionType) type).getUnionList();
                    for (int j = 0; j < unionList.size(); j++)
                        if (unionList.get(j).getTypeTag() == ATypeTag.NULL) {
                            isNull = true;
                            break;
                        }
                    if (!isNull)
                        return i;
                }
            }
        return -1;
    }

    private void parseOrderedList(AOrderedListType oltype, DataOutput out) throws IOException, AsterixException {

        ArrayBackedValueStorage itemBuffer = getTempBuffer();
        OrderedListBuilder orderedListBuilder = (OrderedListBuilder) getOrderedListBuilder();

        IAType itemType = null;
        if (oltype != null)
            itemType = oltype.getItemType();
        orderedListBuilder.reset(oltype);

        Token token = null;
        boolean inList = true;
        boolean expectingListItem = false;
        boolean first = true;
        do {
            token = nextToken();
            if (token.kind == AdmLexerConstants.END_ORDERED_LIST) {
                if (expectingListItem) {
                    throw new AsterixException("Found END_COLLECTION while expecting a list item.");
                }
                inList = false;
            } else if (token.kind == AdmLexerConstants.COMMA) {
                if (first) {
                    throw new AsterixException("Found COMMA before any list item.");
                }
                if (expectingListItem) {
                    throw new AsterixException("Found COMMA while expecting a list item.");
                }
                expectingListItem = true;
            } else {
                expectingListItem = false;
                itemBuffer.reset();

                admFromLexerStream(token, itemType, itemBuffer.getDataOutput(), false);
                orderedListBuilder.addItem(itemBuffer);
            }
            first = false;
        } while (inList);
        orderedListBuilder.write(out, true);
        returnOrderedListBuilder(orderedListBuilder);
        returnTempBuffer(itemBuffer);
    }

    private void parseUnorderedList(AUnorderedListType uoltype, DataOutput out) throws IOException, AsterixException {

        ArrayBackedValueStorage itemBuffer = getTempBuffer();
        UnorderedListBuilder unorderedListBuilder = (UnorderedListBuilder) getUnorderedListBuilder();

        IAType itemType = null;

        if (uoltype != null)
            itemType = uoltype.getItemType();
        unorderedListBuilder.reset(uoltype);

        Token token = null;
        boolean inList = true;
        boolean expectingListItem = false;
        boolean first = true;
        do {
            token = nextToken();
            if (token.kind == AdmLexerConstants.END_UNORDERED_LIST) {
                if (expectingListItem) {
                    throw new AsterixException("Found END_COLLECTION while expecting a list item.");
                }
                inList = false;
            } else if (token.kind == AdmLexerConstants.COMMA) {
                if (first) {
                    throw new AsterixException("Found COMMA before any list item.");
                }
                if (expectingListItem) {
                    throw new AsterixException("Found COMMA while expecting a list item.");
                }
                expectingListItem = true;
            } else {
                expectingListItem = false;
                itemBuffer.reset();
                admFromLexerStream(token, itemType, itemBuffer.getDataOutput(), false);
                unorderedListBuilder.addItem(itemBuffer);
            }
            first = false;
        } while (inList);
        unorderedListBuilder.write(out, true);
        returnUnorderedListBuilder(unorderedListBuilder);
        returnTempBuffer(itemBuffer);
    }

    private Token nextToken() throws AsterixException {
        try {
            return admLexer.next();
        } catch (ParseException pe) {
            throw new AsterixException(pe);
        }
    }

    private IARecordBuilder getRecordBuilder() {
        RecordBuilder recBuilder = (RecordBuilder) recordBuilderPool.poll();
        if (recBuilder != null)
            return recBuilder;
        else
            return new RecordBuilder();
    }

    private void returnRecordBuilder(IARecordBuilder recBuilder) {
        this.recordBuilderPool.add(recBuilder);
    }

    private IAOrderedListBuilder getOrderedListBuilder() {
        OrderedListBuilder orderedListBuilder = (OrderedListBuilder) orderedListBuilderPool.poll();
        if (orderedListBuilder != null)
            return orderedListBuilder;
        else
            return new OrderedListBuilder();
    }

    private void returnOrderedListBuilder(IAOrderedListBuilder orderedListBuilder) {
        this.orderedListBuilderPool.add(orderedListBuilder);
    }

    private IAUnorderedListBuilder getUnorderedListBuilder() {
        UnorderedListBuilder unorderedListBuilder = (UnorderedListBuilder) unorderedListBuilderPool.poll();
        if (unorderedListBuilder != null)
            return unorderedListBuilder;
        else
            return new UnorderedListBuilder();
    }

    private void returnUnorderedListBuilder(IAUnorderedListBuilder unorderedListBuilder) {
        this.unorderedListBuilderPool.add(unorderedListBuilder);
    }

    private ArrayBackedValueStorage getTempBuffer() {
        ArrayBackedValueStorage tmpBaaos = baaosPool.poll();
        if (tmpBaaos != null) {
            return tmpBaaos;
        } else {
            return new ArrayBackedValueStorage();
        }
    }

    private void returnTempBuffer(ArrayBackedValueStorage tempBaaos) {
        baaosPool.add(tempBaaos);
    }

    public RecordDescriptor getRecordDescriptor() {
        return recordDescriptor;
    }

    public IPrinterFactory[] getPrintFactories() {
        return printFactories;
    }

    public int[] getFields() {
        return new int[] { 0, 1, 2, 3 };
    }

}

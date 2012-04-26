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
import java.util.ArrayDeque;
import java.util.Calendar;
import java.util.Queue;

import edu.uci.ics.asterix.adm.parser.nontagged.AdmLexer;
import edu.uci.ics.asterix.builders.IAOrderedListBuilder;
import edu.uci.ics.asterix.builders.IARecordBuilder;
import edu.uci.ics.asterix.builders.IAUnorderedListBuilder;
import edu.uci.ics.asterix.builders.RecordBuilder;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.metadata.MetadataException;
import edu.uci.ics.asterix.metadata.bootstrap.MetadataPrimaryIndexes;
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
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;

/**
 * @author rico
 * 
 */
public class BaseStatisticsTupleTranslator extends AbstractTupleTranslator<BaseStatistics> {

    private AMutableString aString = new AMutableString("");

    @SuppressWarnings("unchecked")
    public ISerializerDeserializer<AString> stringSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.ASTRING);
    @SuppressWarnings("unchecked")
    private ISerializerDeserializer<AInt64> int64Serde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.AINT64);

    // Field indexes of serialized BaseStatistics in a tuple.
    // First key field.
    public static final int DATASET_DATAVERSENAME_TUPLE_FIELD_INDEX = 0;
    // // Second key field.
    public static final int DATASET_DATASETNAME_TUPLE_FIELD_INDEX = 1;
    // Third index field
    public static final int DATASET_NODEID_TUPLE_FIELD_INDEX = 2;
    // Payload field containing serialized Dataset.
    public static final int DATASET_PAYLOAD_TUPLE_FIELD_INDEX = 3;
    @SuppressWarnings("unchecked")
    private ISerializerDeserializer<ARecord> recordSerDes = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(MetadataRecordTypes.BASE_STATISTICS_RECORDTYPE);

    public BaseStatisticsTupleTranslator(boolean getTuple) {
        super(getTuple, MetadataPrimaryIndexes.STATISTICS_DATASET.getFieldCount());
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
        ARecord statsRecord = (ARecord) recordSerDes.deserialize(in);
        return createStatisticsFromARecord(statsRecord);
    }

    private BaseStatistics createStatisticsFromARecord(ARecord statsRecord) {
        String dataverseName = ((AString) statsRecord
                .getValueByPos(MetadataRecordTypes.BASE_STATISTICS_ARECORD_DATAVERSE_INDEX)).getStringValue();
        String datasetName = ((AString) statsRecord
                .getValueByPos(MetadataRecordTypes.BASE_STATISTICS_ARECORD_DATASET_INDEX)).getStringValue();
        String nodeId = ((AString) statsRecord.getValueByPos(MetadataRecordTypes.BASE_STATISTICS_ARECORD_NODEID_INDEX))
                .getStringValue();
        ARecord recordCountRecord = (ARecord) statsRecord
                .getValueByPos(MetadataRecordTypes.BASE_STATISTICS_ARECORD_RECORDCOUNT_INDEX);
        long recordCount = ((AInt64) recordCountRecord
                .getValueByPos(MetadataRecordTypes.RECORDCOUNT_ARECORD_RECORDCOUNT_INDEX)).getLongValue();

        return new BaseStatistics(new AqlSourceId(dataverseName, datasetName), nodeId, recordCount);
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.uci.ics.asterix.metadata.api.IMetadataEntityTupleTranslator#
     * getTupleFromMetadataEntity(java.lang.Object)
     */
    @Override
    public ITupleReference getTupleFromMetadataEntity(BaseStatistics baseStatistics) throws IOException {
        // write the key in the first 3 fields of the tuple
        tupleBuilder.reset();
        aString.setValue(baseStatistics.getAqlSourceId().getDataverseName());
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();
        aString.setValue(baseStatistics.getAqlSourceId().getDatasetName());
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();
        aString.setValue(baseStatistics.getNodeId());
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

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

        IARecordBuilder internalRecordBuilder = new RecordBuilder();
        ArrayBackedValueStorage internalFieldValue = new ArrayBackedValueStorage();
        AMutableInt64 aInt = new AMutableInt64(baseStatistics.getRecordCount());
        internalRecordBuilder.reset(MetadataRecordTypes.RECORDCOUNT_RECORDTYPE);
        internalFieldValue.reset();
        int64Serde.serialize(aInt, internalFieldValue.getDataOutput());
        internalRecordBuilder.addField(MetadataRecordTypes.RECORDCOUNT_ARECORD_RECORDCOUNT_INDEX, internalFieldValue);
        internalRecordBuilder.write(fieldValue.getDataOutput(), true);
        recordBuilder.addField(MetadataRecordTypes.BASE_STATISTICS_ARECORD_RECORDCOUNT_INDEX, fieldValue);

        // write field 4
        fieldValue.reset();
        aString.setValue(Calendar.getInstance().getTime().toString());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(MetadataRecordTypes.BASE_STATISTICS_ARECORD_TIMESTAMP_INDEX, fieldValue);

        // write record
        recordBuilder.write(tupleBuilder.getDataOutput(), true);
        tupleBuilder.addFieldEndOffset();

        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
        return tuple;
    }

}

package edu.uci.ics.asterix.feed.intake;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ARecordSerializerDeserializer;
import edu.uci.ics.asterix.om.base.AMutableRecord;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public abstract class PullBasedFeedClient implements IPullBasedFeedClient {

    protected ARecordType recordType;
    protected ARecordSerializerDeserializer recordSerDe;
    protected AMutableRecord mutableRecord;

    public abstract boolean setNextRecord() throws Exception;

    @Override
    public boolean nextTuple(DataOutput dataOutput) throws AsterixException {
        try {
            boolean newData = setNextRecord();
            if (newData) {
                IAType t = mutableRecord.getType();
                ATypeTag tag = t.getTypeTag();
                try {
                    dataOutput.writeByte(tag.serialize());
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
                recordSerDe.serialize(mutableRecord, dataOutput);
                return true;
            }
            return false;
        } catch (Exception e) {
            throw new AsterixException(e);
        }

    }

    public void displayFeedRecord() {
        StringBuilder builder = new StringBuilder();
        int numFields = recordType.getFieldNames().length;
        for (int i = 0; i < numFields; i++) {
            builder.append(mutableRecord.getValueByPos(i).toString());
            builder.append("|");
        }
    }

}

package edu.uci.ics.asterix.runtime.operators.file;

import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.om.types.ARecordType;

public interface IDataParser {

	public void initialize(InputStream in, ARecordType recordType,
			boolean datasetRec) throws AsterixException, IOException;

	public boolean parse(DataOutput out) throws AsterixException, IOException;
}

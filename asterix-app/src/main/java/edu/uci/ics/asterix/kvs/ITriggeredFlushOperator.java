package edu.uci.ics.asterix.kvs;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public interface ITriggeredFlushOperator {
	
	public void startTriggers();
	
	public void triggeredFlush() throws HyracksDataException;
}

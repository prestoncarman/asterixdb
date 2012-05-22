package edu.uci.ics.asterix.kvs;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;


public interface ITriggeredFlushOperator {
	
	public void startTrigger();
	
	public void triggeredFlush(int arg) throws HyracksDataException;
}

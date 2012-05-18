package edu.uci.ics.asterix.kvs;

import java.util.TimerTask;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class TimeTrigger extends TimerTask {
	
	private final ITriggeredFlushOperator op;
	
	public TimeTrigger(ITriggeredFlushOperator op){
		this.op = op;
	}

	@Override
	public void run() {
		try {
			op.triggeredFlush();
		} catch (HyracksDataException e) {
			System.err.println("Problem in triggered flushing for operator "+op.getClass());
			e.printStackTrace();
		}
	}
}

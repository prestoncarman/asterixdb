package edu.uci.ics.asterix.kvs;

import java.util.concurrent.LinkedBlockingQueue;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;


public class TimeTrigger implements Runnable {
	
	private final ITriggeredFlushOperator op;
	private final LinkedBlockingQueue<Object> queue;
	private long waitTime;
	private int ix;
	private boolean stop;
	
	public TimeTrigger(ITriggeredFlushOperator op, LinkedBlockingQueue<Object> queue){
		setParams(-1, -1);
		this.op = op;
		this.queue = queue;
		stop = false;
	}
	
	public void setParams(long waitTime, int ix){
		this.waitTime = waitTime;
		this.ix = ix;
	}
	
	public void stop(){
		stop = true;
	}
	
	public boolean isRunning(){
		return !stop;
	}
	
	
	@Override
	public void run() {
		while(!stop){
			try {
				queue.take();
				if(waitTime > 0){
					Thread.sleep(waitTime);
					op.triggeredFlush(ix);
				}
			} catch (InterruptedException e) {
				if(stop){
					break;
				}
			} catch (HyracksDataException e) {
				e.printStackTrace();
			}
		}
	}
	
	
}

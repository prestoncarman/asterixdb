package edu.uci.ics.asterix.kvs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.LinkedBlockingQueue;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.comm.IPartitionWriterFactory;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;

public class TimeTriggeredPartitionDataWriter implements ITriggeredFlushOperator, IFrameWriter {
	private final int INVALID = -1;
	private final long maxWaitTime;
	private final int minFlushSize;
	
	private final int consumerPartitionCount;
    private final IFrameWriter[] pWriters;
    private final FrameTupleAppender[] appenders;
    private final FrameTupleAccessor tupleAccessor;
    private final ITuplePartitionComputer tpc;
	
	private final Object signal;
	private final LinkedBlockingQueue<Object> signalChannel;
	private TimeTrigger trigger;
	private Thread triggerThread;
	
	private long[] timeTable;
	private int[] next;
	private int[] prev;
	private int tail;

	public TimeTriggeredPartitionDataWriter(IHyracksTaskContext ctx,
												int consumerPartitionCount, IPartitionWriterFactory pwFactory,
													RecordDescriptor recordDescriptor, ITuplePartitionComputer tpc, long flushTimeTreshold, int flushSizeThreshold) throws HyracksDataException {
		
		this.consumerPartitionCount = consumerPartitionCount;
        pWriters = new IFrameWriter[consumerPartitionCount];
        appenders = new FrameTupleAppender[consumerPartitionCount];
        for (int i = 0; i < consumerPartitionCount; ++i) {
            try {
                pWriters[i] = pwFactory.createFrameWriter(i);
                appenders[i] = new FrameTupleAppender(ctx.getFrameSize());
                appenders[i].reset(ctx.allocateFrame(), true);
            } catch (IOException e) {
                throw new HyracksDataException(e);
            }
        }
        tupleAccessor = new FrameTupleAccessor(ctx.getFrameSize(), recordDescriptor);
        this.tpc = tpc;
		
		this.maxWaitTime = flushTimeTreshold;
		this.minFlushSize = flushSizeThreshold;
		signal = new Object();
		signalChannel = new LinkedBlockingQueue<Object>();
		trigger = new TimeTrigger(this, signalChannel);
		triggerThread = new Thread( trigger );
		
		//System.out.println(">>>> TriggeredDataWriter created with delay "+maxWaitTime);
	}
	
	
	@Override
    public void open() throws HyracksDataException {
		for (int i = 0; i < pWriters.length; ++i) {
            pWriters[i].open();
            appenders[i].reset(appenders[i].getBuffer(), true);
        }
		
		timeTable = new long[consumerPartitionCount];
		next = new int[consumerPartitionCount];
		prev = new int[consumerPartitionCount];
		for(int i=0; i<consumerPartitionCount; i++){
			timeTable[i] = INVALID;
			next[i] = INVALID;
			prev[i] = INVALID;
		}
		tail = INVALID;
		
		startTrigger();
	}
	
	@Override
	public void startTrigger() {
		triggerThread.start();
	}
	
	@Override
    public void close() throws HyracksDataException {
		for (int i = 0; i < pWriters.length; ++i) {
            if (appenders[i].getTupleCount() > 0) {
                flushFrameContent(appenders[i].getBuffer(), pWriters[i]);
            }
            pWriters[i].close();
        }
		trigger.stop();
		triggerThread.interrupt();
	}

	@Override
	public void fail() throws HyracksDataException {
		for (int i = 0; i < appenders.length; ++i) {
			pWriters[i].fail();
		}
		trigger.stop();
		triggerThread.interrupt();
	}


	@Override
	public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
		tupleAccessor.reset(buffer);
        int tupleCount = tupleAccessor.getTupleCount();
        try {
			for (int i = 0; i < tupleCount; ++i) {
			    int h = tpc.partition(tupleAccessor, i, consumerPartitionCount);
			    
			    if(maxWaitTime == 0){		//Added for 0 delay case
			    	noDelayProcess(h, i);
			    	return;
			    }
			    
			    while(!addToFrame(h, i)){
			    	flush(h, false);
			    }
			    
			    if( (minFlushSize > 0) && (getCurrentSize(h) >= minFlushSize) ){
			    	//System.out.println("Size based flush with "+appenders[h].getTupleCount()+" tuples in connector");
			    	flush(h, false);
			    }
			}
		} catch (InterruptedException e) {
			throw new HyracksDataException(e);
		}
	}
	
	
	
	private boolean addToFrame(int fIx, int tIx) throws InterruptedException{
        FrameTupleAppender appender = appenders[fIx];
        if (!appender.append(tupleAccessor, tIx)) {
        	return false;
        }
        if(timeTable[fIx] != INVALID){	//Some older tuple is in the frame
        	return true;
        }
        timeTable[fIx] = System.currentTimeMillis();	//first tuple in this buffer
        prev[fIx] = tail;
        next[fIx] = INVALID;
        if(tail == INVALID){	//No task already scheduled
        	trigger.setParams(maxWaitTime, fIx);
        	tail = fIx;
        	signalChannel.put(signal);
        }
        else{	//(Some task already scheduled) Append to the end of list
        	next[tail] = fIx;
        	tail = fIx;
        }
        return true;
	}
	
	private void followChain(int start) throws InterruptedException, HyracksDataException{
		if(start != INVALID ){
			long w = System.currentTimeMillis() - timeTable[start]; 
			if( w < maxWaitTime){
				trigger.setParams((maxWaitTime - w), start);
				signalChannel.put(signal);
				return;
			}
			else{
				int n = next[start];
				flushFrame(start);
				followChain(n);
			}
		}
		else{
			tail = INVALID;		//It was the last
		}
	}
	
	@Override
	public void triggeredFlush(int fIx) throws HyracksDataException{
		flush(fIx, true);
	}
	
	
	public void flush(int fIx, boolean fromTrigger) throws HyracksDataException{
		try {
			int n = next[fIx];
			int p = prev[fIx];
			flushFrame(fIx);
			if(p == INVALID){	//it was the scheduled task
				if(!fromTrigger){
					trigger.setParams(INVALID, INVALID);
					triggerThread.interrupt();
				}
				followChain(n);
			}
			else{		//It was in the middle of chain
				next[p] = n;
				if(n != INVALID){
					prev[n] = p;
				}
				else{	//It was the very last in the chain
					tail = p;
				}
			}
		}  catch (InterruptedException e) {
			throw new HyracksDataException(e);
		}
	}
	
	private void flushFrame(int ix) throws HyracksDataException {
		if (appenders[ix].getTupleCount() > 0) {
            try {
            	ByteBuffer buff = appenders[ix].getBuffer();
            	synchronized(buff){
            		flushFrameContent(buff, pWriters[ix]);
    				appenders[ix].reset(buff, true);
    				timeTable[ix] = INVALID;
    				next[ix] = INVALID;
    				prev[ix] = INVALID;
            	}
			} catch (HyracksDataException e) {
				throw new HyracksDataException(e); 
			}
        }
		
	}
	
	
	private void flushFrameContent(ByteBuffer buffer, IFrameWriter frameWriter) throws HyracksDataException {
        buffer.position(0);
        buffer.limit(buffer.capacity());
        frameWriter.nextFrame(buffer);
    }
	
	private int getCurrentSize(int ix){
		return appenders[ix].getTupleCount();
	}
	
	
	private void noDelayProcess(int fIx, int tIx) throws HyracksDataException{
		FrameTupleAppender appender = appenders[fIx];
		if (!appender.append(tupleAccessor, tIx)) {
			throw new IllegalStateException();
		}
		ByteBuffer buff = appender.getBuffer();
		flushFrameContent(buff, pWriters[fIx]);
		appender.reset(buff, true);
	}
	
	
	
}
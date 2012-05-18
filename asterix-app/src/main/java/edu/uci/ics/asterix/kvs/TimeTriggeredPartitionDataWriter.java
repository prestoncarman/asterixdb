package edu.uci.ics.asterix.kvs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Timer;
import java.util.TimerTask;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.comm.IPartitionWriterFactory;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;

public class TimeTriggeredPartitionDataWriter implements IFrameWriter {
	
	private final int consumerPartitionCount;
    private final IFrameWriter[] pWriters;
    private final FrameTupleAppender[] appenders;
    private final FrameTupleAccessor tupleAccessor;
    private final ITuplePartitionComputer tpc;
	
	private final Timer timer;
	private TimerTask[] triggers;
	private final long flushPeriod;
	

	public TimeTriggeredPartitionDataWriter(IHyracksTaskContext ctx,
												int consumerPartitionCount, IPartitionWriterFactory pwFactory,
													RecordDescriptor recordDescriptor, ITuplePartitionComputer tpc, long flushPeriod) throws HyracksDataException {
		
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
		
		timer = new Timer();
		triggers = new Trigger[consumerPartitionCount];
		this.flushPeriod = flushPeriod;
	}
	
	
	@Override
    public void open() throws HyracksDataException {
		for (int i = 0; i < pWriters.length; ++i) {
            pWriters[i].open();
            appenders[i].reset(appenders[i].getBuffer(), true);
        }
		
		for(int i=0; i<consumerPartitionCount; i++){
			triggers[i] = new Trigger(this, i);
			timer.scheduleAtFixedRate(triggers[i], 0, flushPeriod);
			System.out.println(">>>>>> (Inside TimeTriggeredConnector) - Task for writer "+i+" scheduled for interval "+triggers[i].scheduledExecutionTime());
		}
	}
	
	@Override
    public void close() throws HyracksDataException {
		for (int i = 0; i < pWriters.length; ++i) {
            if (appenders[i].getTupleCount() > 0) {
                flushFrame(appenders[i].getBuffer(), pWriters[i]);
            }
            pWriters[i].close();
        }
		
		timer.cancel();
		triggers = null;
	}
	
	protected void flushFrame(ByteBuffer buffer, IFrameWriter frameWriter) throws HyracksDataException {
        buffer.position(0);
        buffer.limit(buffer.capacity());
        frameWriter.nextFrame(buffer);
    }
	
	protected void triggeredFlush(int bufferIx) throws HyracksDataException{
		if (appenders[bufferIx].getTupleCount() > 0) {
            try {
            	ByteBuffer buff = appenders[bufferIx].getBuffer();
				flushFrame(buff, pWriters[bufferIx]);
				appenders[bufferIx].reset(buff, true);
			} catch (HyracksDataException e) {
				throw new HyracksDataException(e); 
			}
        }
	}

	@Override
	public void fail() throws HyracksDataException {
		for (int i = 0; i < appenders.length; ++i) {
			pWriters[i].fail();
		}
	}


	@Override
	public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
		tupleAccessor.reset(buffer);
        int tupleCount = tupleAccessor.getTupleCount();
        for (int i = 0; i < tupleCount; ++i) {
            int h = tpc.partition(tupleAccessor, i, consumerPartitionCount);
            FrameTupleAppender appender = appenders[h];
            if (!appender.append(tupleAccessor, i)) {
                ByteBuffer appenderBuffer = appender.getBuffer();
                flushFrame(appenderBuffer, pWriters[h]);
                appender.reset(appenderBuffer, true);
                if (!appender.append(tupleAccessor, i)) {
                    throw new IllegalStateException();
                }
            }
        }
	}
}

class Trigger extends TimerTask {
	
	private final TimeTriggeredPartitionDataWriter pdWriter;
	private final int bufferIx;
	
	public Trigger(TimeTriggeredPartitionDataWriter pdw, int bIx){
		pdWriter = pdw;
		bufferIx = bIx;
	}

	@Override
	public void run() {
		try {
			pdWriter.triggeredFlush(bufferIx);
		} catch (HyracksDataException e) {
			System.err.println("Problem in triggered flushing into PartitionWriter "+bufferIx);
			e.printStackTrace();
		}
	}
}

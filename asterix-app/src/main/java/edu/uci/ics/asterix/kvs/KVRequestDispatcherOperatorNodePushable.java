package edu.uci.ics.asterix.kvs;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import edu.uci.ics.asterix.om.base.AInt32;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;

public class KVRequestDispatcherOperatorNodePushable extends AbstractUnaryOutputSourceOperatorNodePushable implements ITriggeredFlushOperator {
	private final int INVALID = -1;
	private final int MESSAGE_AS_RESULT = KVUtils.KVResponseType.MESSAGE.ordinal();
	
	private final KVCallParser callParser;
	private final int pId;
	private final LinkedBlockingQueue<IKVCall> queue;
	private FrameTupleAppender appender;
	private ByteBuffer frame;
	
	private final long maxWaitTime;
	private final int minFlushSize;
	private final TimeTrigger trigger;
	private final Thread triggerThread;
	private final Object signal;
	private final LinkedBlockingQueue<Object> signalChannel;
	private long scheduledTime;

	
	public KVRequestDispatcherOperatorNodePushable(IHyracksTaskContext ctx, int partition, IAType[] keyType, KVServiceID sId, ARecordType record, List<String> partitionKeys, long maxWaitTime, int flushSize, RecordDescriptor onlyValueRDesc){
		pId = partition;
		queue = new LinkedBlockingQueue<IKVCall>();
		callParser = new KVCallParser(ctx, this, keyType, pId, partitionKeys, record, onlyValueRDesc);
		frame = ctx.allocateFrame();
        appender = new FrameTupleAppender(ctx.getFrameSize());
        appender.reset(frame, true);
        //String ncId = ctx.getJobletContext().getApplicationContext().getNodeId(); //<<< How to get nodeId
        KVServiceProvider.INSTANCE.registerQueryQueue(sId, queue, record);
        this.maxWaitTime = maxWaitTime;
        this.minFlushSize = flushSize;
        signal = new Object();
        signalChannel = new LinkedBlockingQueue<Object>();
        trigger = new TimeTrigger(this, signalChannel);
        triggerThread = new Thread( trigger );
        scheduledTime = INVALID;
        
        System.out.println(">>>> Request Dispatcher created with delay "+maxWaitTime);
	}
	
	@Override
    public void initialize() throws HyracksDataException {
		try {
			writer.open();
			startTrigger();
			while(true){
				IKVCall call = queue.take();
				System.out.println(">>> A new call of type "+call.getType()+" received from query queue in partition "+pId);
				
				switch(call.getType() ){
				case GET:
					callParser.processGetCall( (GetCall) call );
					break;
				
				case PUT:
					callParser.processPutCall( (PutCall) call );
					break;
				
				default:
					throw new IllegalStateException("Invalid KV Call Type");	//TODO Revise exception type
				}
			}
		} catch (Exception e) {
			throw new HyracksDataException(e);
		} finally {
			if(trigger.isRunning()){
				trigger.stop();
				triggerThread.interrupt();
			}
            writer.close();
        }
    }
	
	public void addTuples(int[] fieldSlots, byte[] bytes, int offset, int length) throws HyracksDataException{
		//synchronized(frame){
			if(maxWaitTime == 0){
				if(!appender.append(fieldSlots, bytes, offset, length)){
					throw new IllegalStateException();
				}
				FrameUtils.flushFrame(frame, writer);
				appender.reset(frame, true);
				return;
			}
		
			while ( !appender.append(fieldSlots, bytes, offset, length) ) {
				flush(false);
			}
			
			if( (minFlushSize > 0) && (appender.getTupleCount() >= minFlushSize) ){
				System.out.println("Size based flush with "+appender.getTupleCount()+" tuples in reqDispatcher");
				flush(false);
				return;
			}
			
			if(scheduledTime == INVALID){
				try {
					scheduledTime = maxWaitTime;
					trigger.setParams(scheduledTime, 0);
					signalChannel.put(signal);
				} catch (InterruptedException e) {
					throw new HyracksDataException(e);
				}
			}
		//}
	}
	
	
	
	public void flush(boolean fromTrigger) throws HyracksDataException{ 
		synchronized (frame) {
			if(appender.getTupleCount() > 0){
				System.out.println("Flushing "+appender.getTupleCount()+" Tuples");
				FrameUtils.flushFrame(frame, writer);
				appender.reset(frame, true);
			}
			if(!fromTrigger){
				trigger.setParams(INVALID, INVALID);
				triggerThread.interrupt();
			}
			scheduledTime = INVALID;
		}
	}
	
	public KVCallParser getKVManager(){
		return callParser;
	}


	@Override
	public void startTrigger() {
		triggerThread.start();
		
	}

	@Override
	public void triggeredFlush(int arg) throws HyracksDataException {
		flush(true);
	}
	
	public void reportMissingKeysError(int queryId, List<String> missedColumns) throws InterruptedException{
		LinkedBlockingQueue<Object[]> outputQueue = KVServiceProvider.INSTANCE.getOutputQueue(queryId);
		Object[] errorMessage = new Object[4];
		errorMessage[0] = new AInt32(pId);
		errorMessage[1] = new AInt32(queryId);
		errorMessage[2] = new AInt32(MESSAGE_AS_RESULT);
		StringBuffer st = new StringBuffer();
		st.append("Value missed for key field(s):\t");
		for(String s : missedColumns){
			st.append(s+", ");
		}
		errorMessage[3] = st.toString();
		outputQueue.put(errorMessage);
	}
}
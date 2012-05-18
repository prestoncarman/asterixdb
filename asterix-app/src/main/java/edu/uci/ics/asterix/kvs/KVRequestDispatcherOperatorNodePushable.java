package edu.uci.ics.asterix.kvs;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Timer;
import java.util.concurrent.LinkedBlockingQueue;

import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
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
	
	private final KVCallParser callParser;
	private final int pId;
	private final LinkedBlockingQueue<IKVCall> queue;
	private FrameTupleAppender appender;
	private ByteBuffer frame;
	
	private final long flushPeriod;
	private final Timer timer;
	private final TimeTrigger trigger;

	
	public KVRequestDispatcherOperatorNodePushable(IHyracksTaskContext ctx, int partition, IAType[] keyType, KVServiceID sId, ARecordType record, List<String> partitionKeys, long flushPeriod, RecordDescriptor onlyValueRDesc){
		pId = partition;
		queue = new LinkedBlockingQueue<IKVCall>();
		callParser = new KVCallParser(ctx, this, keyType, pId, partitionKeys, record, onlyValueRDesc);
		frame = ctx.allocateFrame();
        appender = new FrameTupleAppender(ctx.getFrameSize());
        appender.reset(frame, true);
        //String ncId = ctx.getJobletContext().getApplicationContext().getNodeId(); //<<< How to get nodeId
        KVServiceProvider.INSTANCE.registerQueryQueue(sId, queue, record);
        timer = new Timer();
        trigger = new TimeTrigger(this);
        this.flushPeriod = flushPeriod;
	}
	
	@Override
    public void initialize() throws HyracksDataException {
		try {
			writer.open();
			startTriggers();
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
			timer.cancel();
            writer.close();
        }
    }
	
	public void addTuples(int[] fieldSlots, byte[] bytes, int offset, int length) throws HyracksDataException{
		synchronized(frame){
			if ( !appender.append(fieldSlots, bytes, offset, length) ) {
				flush();
				if (!appender.append(fieldSlots, bytes, offset, length) ) {
					throw new IllegalStateException();
				}
			}
		}
	}
	
	public void reportMissingKeysError(int queryId, List<String> missedColumns) throws InterruptedException{
		LinkedBlockingQueue<Object[]> outputQueue = KVServiceProvider.INSTANCE.getOutputQueue(queryId);
		Object[] errorMessage = new Object[3];
		errorMessage[0] = new AInt32(pId);
		errorMessage[1] = new AInt32(queryId);
		StringBuffer st = new StringBuffer();
		st.append("Value missed for key fields:\t");
		for(String s : missedColumns){
			st.append(s+", ");
		}
		errorMessage[3] = st.toString();
		outputQueue.put(errorMessage);
	}
	
	public void flush() throws HyracksDataException{ 
		synchronized (frame) {
			if(appender.getTupleCount() > 0){
				System.out.println("Flushing "+appender.getTupleCount()+" Tuples");
				FrameUtils.flushFrame(frame, writer);
				appender.reset(frame, true);
			}
		}
	}
	
	public KVCallParser getKVManager(){
		return callParser;
	}

	@Override
	public void startTriggers() {
		timer.scheduleAtFixedRate(trigger, 0, flushPeriod);
		
	}

	@Override
	public void triggeredFlush() throws HyracksDataException {
		flush();
	}
}
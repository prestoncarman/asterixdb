package edu.uci.ics.asterix.kvs;

import java.nio.ByteBuffer;
import java.util.concurrent.LinkedBlockingQueue;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameDeserializer;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;

public class KVSResponseDispatcherOperatorNodePushable extends AbstractUnaryInputSinkOperatorNodePushable {
	private final int QUERY_ID_INDEX = 1;	//Index of field containing query id 
	
	private final IHyracksTaskContext ctx;
	private final RecordDescriptor recDesc;
	private final FrameDeserializer frameDeser;
	private FrameTupleAccessor accessor;
	private FrameTupleReference tr;
	
	public KVSResponseDispatcherOperatorNodePushable(IHyracksTaskContext ctx, AbstractSingleActivityOperatorDescriptor opDesc, final IRecordDescriptorProvider recordDescProvider){
		this.ctx = ctx;
		recDesc = recordDescProvider.getInputRecordDescriptor(opDesc.getOperatorId(), 0);
		frameDeser = new FrameDeserializer(ctx.getFrameSize(), recDesc);
	}
	
	@Override
	public void open() throws HyracksDataException {
		accessor = new FrameTupleAccessor(ctx.getFrameSize(), recDesc);
		tr = new FrameTupleReference();	//TODO Do we really need tr (Can we just use accessor)
	}

	@Override
	public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
		accessor.reset(buffer);
		int tupleCount = accessor.getTupleCount();
		frameDeser.reset(buffer);	//TODO see if we can reuse the fta in FrameDeserializer (dropping accessor)
		for(int i=0; i<tupleCount; i++){
			tr.reset(accessor, i);
			int qIdStart = (tr.getFieldStart(QUERY_ID_INDEX)) + 1;		//+1 added because of SerDe change
			int qId = ( accessor.getBuffer() ).getInt(qIdStart);
			
			LinkedBlockingQueue<Object[]> outputQueue = KVServiceProvider.INSTANCE.getOutputQueue(qId);
			if(outputQueue == null){
				throw new IllegalStateException("No queue for query "+qId);
			}
			try {
				outputQueue.put( frameDeser.deserializeRecord() );
			} catch (InterruptedException e) {
				System.err.println("Error in Deserializing Record in FrameDeser in ResponseDispatcher");	//TODO Revise the exception and error message
				e.printStackTrace();
			}
		}
	}

	@Override
	public void fail() throws HyracksDataException {
	}

	@Override
	public void close() throws HyracksDataException {
		frameDeser.close();
	}
}
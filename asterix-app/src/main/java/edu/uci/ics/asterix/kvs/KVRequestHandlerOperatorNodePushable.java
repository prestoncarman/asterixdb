package edu.uci.ics.asterix.kvs;

import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Timer;
import java.util.concurrent.LinkedBlockingQueue;

import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.kvs.IKVCall.KVCallType;
import edu.uci.ics.asterix.om.base.AInt32;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTreeRangeSearchCursor;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangePredicate;
import edu.uci.ics.hyracks.storage.am.btree.util.BTreeUtils;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndex;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrame;
import edu.uci.ics.hyracks.storage.am.common.dataflow.AbstractTreeIndexOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.dataflow.PermutingFrameTupleReference;
import edu.uci.ics.hyracks.storage.am.common.dataflow.TreeIndexDataflowHelper;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;

public class KVRequestHandlerOperatorNodePushable extends AbstractUnaryInputUnaryOutputOperatorNodePushable implements ITriggeredFlushOperator{
	private final int INVALID = -1;
	
	private final KVCallType[] callTypes;		//TODO You may want to replace it with a better technique
	TreeIndexDataflowHelper treeIndexHelper;
	private BTreeCallHandler kvsCallHandler;
	RecordDescriptor recDesc;
	FrameTupleAccessor accessor;
	FrameTupleAppender appender;
	ByteBuffer writeBuffer;
	FrameTupleReference tr;
	int partition;
	int numOfKeys;
	AbstractTreeIndexOperatorDescriptor opDesc;
	IHyracksTaskContext ctx;
	
	boolean init;
	
	private final long maxWaitTime;
	private final TimeTrigger trigger;
	private final Thread triggerThread;
	private final Object signal;
	private final LinkedBlockingQueue<Object> signalChannel;
	private long scheduledTime;
	
	public KVRequestHandlerOperatorNodePushable(AbstractTreeIndexOperatorDescriptor opDesc, IHyracksTaskContext ctx, int partition, IRecordDescriptorProvider recordDescProvider, int numOfKeys, long maxWaitTime){
		this.numOfKeys = numOfKeys;
		this.opDesc = opDesc;
		this.ctx = ctx;
		init = false;
		callTypes = KVCallType.values();
		recDesc = recordDescProvider.getInputRecordDescriptor(opDesc.getOperatorId(), 0);
		this.partition = partition;
		
		this.maxWaitTime = maxWaitTime;
		signal = new Object();
		signalChannel = new LinkedBlockingQueue<Object>();
		trigger = new TimeTrigger(this, signalChannel);
		triggerThread = new Thread( trigger );
		scheduledTime = INVALID;
	}
	
	@Override
	public void open() throws HyracksDataException {
		startTrigger();
	}

	@Override
	public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
		if(!init){
			init();
		}
		accessor.reset(buffer);
		int tupleCount = accessor.getTupleCount();
		System.out.println(">>>> ReqHandler received a frame with "+tupleCount+" tuples in partition "+partition);
		for(int i=0; i<tupleCount; i++){
			int qPid = readQueryPid(i);
			System.out.println(">>>> Inside ReqHandler queryPid is "+qPid);
			int qId = readQueryId(i);
			System.out.println(">>>> Inside ReqHandler queryId is "+qId);
			KVCallType type = readCallType(i);
			System.out.println(">>>> Inside ReqHandler queryType is "+type);
			switch(type){
			case PUT:
				System.out.println(">>> It is a putCall in reqHandler");
				kvsCallHandler.insert(qPid, qId, accessor, i);
				break;
			case GET:
				System.out.println(">>> It is a getCall in reqHandler");
				kvsCallHandler.searchTree(qPid, qId, accessor, i);
				break;
			}
		}
	}
	
	private void init() throws HyracksDataException{
		treeIndexHelper = (TreeIndexDataflowHelper) opDesc.getIndexDataflowHelperFactory().createIndexDataflowHelper(opDesc, ctx, partition);
		treeIndexHelper.init(false);
		kvsCallHandler = new BTreeCallHandler(opDesc, this, partition, numOfKeys);
		writeBuffer = treeIndexHelper.getHyracksTaskContext().allocateFrame();
		writer.open();
		accessor = new FrameTupleAccessor(treeIndexHelper.getHyracksTaskContext().getFrameSize(), recDesc);
		appender = new FrameTupleAppender(treeIndexHelper.getHyracksTaskContext().getFrameSize());
        appender.reset(writeBuffer, true);
		tr = new FrameTupleReference();
		kvsCallHandler.init(treeIndexHelper);
		init = true;
	}
	
	private int readQueryPid(int tupleIx){
		tr.reset(accessor, tupleIx);
		int idStart = tr.getFieldStart(0) + 1;		//+1 added because of SerDe change
		return ( accessor.getBuffer() ).getInt(idStart);
	}
	
	private int readQueryId(int tupleIx){
		tr.reset(accessor, tupleIx);
		int idStart = tr.getFieldStart(1) + 1;		//+1 added because of SerDe change
		return ( accessor.getBuffer() ).getInt(idStart);
	}
	
	private KVCallType readCallType(int tupleIx) throws HyracksDataException{
		tr.reset(accessor, tupleIx);
		int typeStart = tr.getFieldStart(2) + 1;	//+1 added because of SerDe change
		int t = ( accessor.getBuffer() ).getInt(typeStart);	
		
		try {
			return callTypes[t];
		} catch (ArrayIndexOutOfBoundsException e) {
			throw new HyracksDataException("Invalid KVCall Type "+t);	//TODO You may want to change it
		}
	}
	
	@Override
	public void fail() throws HyracksDataException {
		 writer.fail();
	}

	@Override
	public void close() throws HyracksDataException {
		if(appender.getTupleCount()>0){
			flush(false);
		}
		kvsCallHandler.close();
		if(trigger.isRunning()){
			trigger.stop();
			triggerThread.interrupt();
		}
		writer.close();
	}
	
	public void appendToResults( ArrayTupleBuilder tb, int[] fieldSlots, byte[] bytes, int offset, int length) throws HyracksDataException{
		//synchronized (writeBuffer) {
			while (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
	            flush(false);
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
	
	private void flush(boolean fromTrigger) throws HyracksDataException{
		synchronized(writeBuffer){
			if(appender.getTupleCount()>0){
				FrameUtils.flushFrame(writeBuffer, writer);
		        appender.reset(writeBuffer, true);
			}
			if(!fromTrigger){
				trigger.setParams(INVALID, INVALID);
				triggerThread.interrupt();
			}
			scheduledTime = INVALID;
		}
	}


	@Override
	public void startTrigger() {
		triggerThread.start();
		
	}

	@Override
	public void triggeredFlush(int arg) throws HyracksDataException {
		flush(true);
	}
}

	
class BTreeCallHandler implements Serializable{
	private static final long serialVersionUID = 1L;
	final private AbstractTreeIndexOperatorDescriptor opDesc;
	final private KVRequestHandlerOperatorNodePushable caller;
	final private int[] keysIndex;
	final private int partitionId;	//Added for debug, remove ultimately
	private TreeIndexDataflowHelper treeIndexHelper;
	private BTree btree;
	private ITreeIndexAccessor searchIndexAccessor;
	private IIndexAccessor insertIndexAccessor;
	private ITreeIndexCursor cursor;
	private ITreeIndexFrame cursorFrame;
	private RangePredicate rangePred;
	private PermutingFrameTupleReference lowKey;
	private PermutingFrameTupleReference highKey;
	private ArrayTupleBuilder tb;
	private DataOutput dos;
		//For put
	private PermutingFrameTupleReference putTuple;
	private int[] putPermutation;
	
	
	public BTreeCallHandler(AbstractTreeIndexOperatorDescriptor opDesc, KVRequestHandlerOperatorNodePushable caller, int pid, int numOfKeys){
		this.opDesc = opDesc;
		this.partitionId = pid;
		this.keysIndex = new int[numOfKeys];
		this.putPermutation = new int[numOfKeys + 1];
		for(int i=0; i<numOfKeys; i++){
			keysIndex[i] = 3+i;
			putPermutation[i] = 3+i;
		}
		putPermutation[numOfKeys] = 3 + numOfKeys;
		this.caller = caller;
	}
	
	public void init(TreeIndexDataflowHelper treeIxHelper) throws HyracksDataException {
		treeIndexHelper = treeIxHelper;
		initForSearch();
		initForInsert();
		ITreeIndex treeIndex = (ITreeIndex) treeIndexHelper.getIndex();
		cursorFrame = treeIndex.getLeafFrameFactory().createFrame();
		cursor = new BTreeRangeSearchCursor((IBTreeLeafFrame) cursorFrame, false);
		lowKey = new PermutingFrameTupleReference();
        lowKey.setFieldPermutation(keysIndex);
        highKey = new PermutingFrameTupleReference();
        highKey.setFieldPermutation(keysIndex);
        MultiComparator lowKeySearchCmp = BTreeUtils.getSearchMultiComparator(treeIndex.getComparatorFactories(), lowKey);
    	MultiComparator highKeySearchCmp = BTreeUtils.getSearchMultiComparator(treeIndex.getComparatorFactories(), highKey);;
        //rangePred = new RangePredicate(true, null, null, true, true, lowKeySearchCmp, highKeySearchCmp);
        rangePred = new RangePredicate(lowKey, highKey, true, true, lowKeySearchCmp, highKeySearchCmp);
    	System.out.println("BTree Field Count:\t"+btree.getFieldCount());
        tb = new ArrayTupleBuilder(2 + btree.getFieldCount());	//We add query-pid and query-id for returned results
        dos = tb.getDataOutput();
        putTuple = new PermutingFrameTupleReference();
		putTuple.setFieldPermutation(putPermutation);
	}
	
	private void initForSearch(){
		btree = (BTree) treeIndexHelper.getIndex();
		searchIndexAccessor = btree.createAccessor();
	}
	
	private void initForInsert() throws HyracksDataException{
		treeIndexHelper.getIndex().open(treeIndexHelper.getIndexFileId());
		insertIndexAccessor = ((ITreeIndex) treeIndexHelper.getIndex()).createAccessor();
	}
	
	public void searchTree(int queryPId, int queryId, IFrameTupleAccessor fta, int tupleIx) throws HyracksDataException {
		try {
			System.out.println("Searching (QID:\t"+queryId+") in partition "+partitionId);
			lowKey.reset(fta, tupleIx);
			highKey.reset(fta, tupleIx);
			rangePred.setLowKey(lowKey, true);
			rangePred.setHighKey(highKey, true);
			cursor.reset();
			//((BTreeRangeSearchCursor) cursor).setDebugFlag(true);
			searchIndexAccessor.search(cursor, rangePred);
			writeSearchResults(queryPId, queryId);
			cursor.reset();
		} catch (Exception e) {
			throw new HyracksDataException(e);
		}
	}
	
	public void insert(int queryPId, int queryId, IFrameTupleAccessor fta, int tupleIx){
		System.out.println(">>>>> Putting (QID:\t"+queryId+") in partition "+partitionId);
		putTuple.reset(fta, tupleIx);
		try {
			insertIndexAccessor.insert(putTuple);
			writeInsertResults(queryPId, queryId);
		} catch (Exception e) {
			e.printStackTrace();
		} 
	}
	
	private void writeSearchResults(int queryPId, int queryId) throws Exception{
		System.out.println(">>>>>>> Writting Results for "+queryId+", cursor has next is "+cursor.hasNext());
		while (cursor.hasNext()) {
            tb.reset();
            cursor.next();
            	//Adding Query-PID
            AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT32).serialize(new AInt32(queryPId), dos);
            tb.addFieldEndOffset();
            	//Adding Query-ID
			AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT32).serialize(new AInt32(queryId), dos);
            tb.addFieldEndOffset();
            
				//Adding Actual Result
            ITupleReference tuple = cursor.getTuple();
            for (int i = 0; i < tuple.getFieldCount(); i++) {
                dos.write(tuple.getFieldData(i), tuple.getFieldStart(i), tuple.getFieldLength(i));
                tb.addFieldEndOffset();
            }
            caller.appendToResults(tb, tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize());
        }
	}
	
	private void writeInsertResults(int queryPId, int queryId) throws IOException{
		System.out.println(">>>>>>> Writting Insert Results for "+queryId);
		tb.reset();
			//Adding Query-PID
        AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT32).serialize(new AInt32(queryPId), dos);
        tb.addFieldEndOffset();
        	//Adding Query-ID
		AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT32).serialize(new AInt32(queryId), dos);
        tb.addFieldEndOffset();
        
        for (int i = 0; i < putTuple.getFieldCount(); i++) {
            dos.write(putTuple.getFieldData(i), putTuple.getFieldStart(i), putTuple.getFieldLength(i));
            tb.addFieldEndOffset();
        }
        
        caller.appendToResults(tb, tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize());
	}
	
	public void close() throws HyracksDataException{
		try {
            try {
                cursor.close();
            } catch (Exception e) {
                throw new HyracksDataException(e);
            }
        } finally {
            treeIndexHelper.deinit();
        }
	}
}
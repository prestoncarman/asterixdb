package edu.uci.ics.asterix.kvs;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.httpclient.methods.multipart.StringPart;

import edu.uci.ics.asterix.builders.IARecordBuilder;
import edu.uci.ics.asterix.builders.RecordBuilder;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AInt64SerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ANullSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ARecordSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AStringSerializerDeserializer;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.formats.nontagged.AqlTypeTraitProvider;
import edu.uci.ics.asterix.om.base.ABoolean;
import edu.uci.ics.asterix.om.base.ADouble;
import edu.uci.ics.asterix.om.base.AFloat;
import edu.uci.ics.asterix.om.base.AInt16;
import edu.uci.ics.asterix.om.base.AInt32;
import edu.uci.ics.asterix.om.base.AInt64;
import edu.uci.ics.asterix.om.base.AInt8;
import edu.uci.ics.asterix.om.base.AMutableString;
import edu.uci.ics.asterix.om.base.ANull;
import edu.uci.ics.asterix.om.base.ARecord;
import edu.uci.ics.asterix.om.base.AString;
import edu.uci.ics.asterix.om.base.IAObject;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.AbstractComplexType;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.EnumDeserializer;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.util.NonTaggedFormatUtil;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.FrameTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.DoubleParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.FloatParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IValueParser;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IValueParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IntegerParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.LongParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.UTF8StringParserFactory;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParser;


public class KVCallParser implements Serializable {
	
	private static final long serialVersionUID = 1L;
	private final int getCallTag = IKVCall.KVCallType.GET.ordinal();
	private final int putCallTag = IKVCall.KVCallType.PUT.ordinal();
	
	IHyracksTaskContext ctx;
	final IAType[] keyType;
	final List<String> partitionKeys;
	final KVRequestDispatcherOperatorNodePushable op;
	final int partitionId;
	final ARecordType record;
	final int[] keyAttsPos;
	
	final RecordDescriptor onlyValueRecDesc;	//Used for extracting keys from the put call
	
	private AdmFullRecordParser valueParser; 
	
	public KVCallParser(IHyracksTaskContext ctx, KVRequestDispatcherOperatorNodePushable op, IAType[] keyType, int pId, List<String> partitionKeys, ARecordType record, RecordDescriptor onlyValueRecDesc) {
		this.ctx = ctx;
		this.op = op;
		this.keyType = keyType;
		this.partitionKeys = partitionKeys;
		this.partitionId = pId;
		this.record = record;
		this.onlyValueRecDesc = onlyValueRecDesc;
		this.keyAttsPos = new int[keyType.length];
		for(int i=0; i<partitionKeys.size(); i++){
			keyAttsPos[i] = record.findFieldPosition(partitionKeys.get(i));
		}
		this.valueParser = new AdmFullRecordParser(record);
	}
	
	
	public void processGetCall(GetCall call) throws Exception{
		List< Pair<String, String> > query = call.getKeys();
		ArrayList<IAObject> keys = new ArrayList<IAObject>();	//TODO Revise It (Not use tokenizer, Support any type, not just AString)
		int kIx = 0;
		List<String> missedColumns = null;	//created only fi some key-col value is missed
		for(String attName : partitionKeys){
			String attValue = getLiteratForField(attName, query);
			if(attValue==null){		//Some key column is missed in query
				if(missedColumns==null){
					missedColumns = new LinkedList<String>();
				}
				missedColumns.add(attName);
			}
			else{
				keys.add( parseLiteral(attValue, keyType[kIx]) );
			}
			kIx++;
		}
		
		if(missedColumns != null){
			op.reportMissingKeysError(call.getId(), missedColumns);
			return;
		}
		
		ArrayTupleBuilder tb = new ArrayTupleBuilder(3 + keyType.length + 1);	
        DataOutput dos = tb.getDataOutput();
        ISerializerDeserializer[] keySerDes = getKeySerDeser();
        tb.reset();
     	//Adding Partition-ID
        AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT32).serialize(new AInt32(partitionId), dos);
        tb.addFieldEndOffset();
        	//Adding Query-ID
        AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT32).serialize(new AInt32(call.getId()), dos);
        tb.addFieldEndOffset();
        	//Adding Q-Type
        AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT32).serialize(new AInt32( getCallTag ), dos);		//TODO Use a cleaner way to get code for GET
        tb.addFieldEndOffset();
        	//Adding Keys
        for(int i=0; i<keys.size(); i++){
        	keySerDes[i].serialize(keys.get(i), dos);
            tb.addFieldEndOffset();
        }
        	//Adding Value (Which is empty in case of GET)
        tb.addFieldEndOffset();
        
        System.out.println(">> Processing GET call in kvmgr in partition "+partitionId);
        
        op.addTuples(tb.getFieldEndOffsets(),tb.getByteArray(), 0, tb.getSize());
	}
	
	public void processPutCall(PutCall call) throws HyracksDataException, FileNotFoundException{
		
		//--------------------
		IAObject[] keyFields = new IAObject[keyType.length];
		String admQuery = call.getRecord();
		ArrayTupleBuilder auxTb = new ArrayTupleBuilder(1);
		DataOutput auxDos = auxTb.getDataOutput();
		auxTb.reset();
		ByteArrayInputStream inputStream = new ByteArrayInputStream(admQuery.getBytes());
		valueParser.parseOneRecord(inputStream, auxTb, auxDos);
		ARecord r = extractRecord(auxTb);
		for(int kIx=0; kIx<keyAttsPos.length; kIx++){
			keyFields[kIx] = r.getValueByPos(keyAttsPos[kIx]);
		}
		//--------------------
		
		ArrayTupleBuilder tb = new ArrayTupleBuilder(3 + keyType.length + 1);	
        DataOutput dos = tb.getDataOutput();
        ISerializerDeserializer[] keySerDes = getKeySerDeser();
        tb.reset();
     		//Adding Partition-ID
        AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT32).serialize(new AInt32(partitionId), dos);
        tb.addFieldEndOffset();
        	//Adding Query-ID
        AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT32).serialize(new AInt32(call.getId()), dos);
        tb.addFieldEndOffset();
        	//Adding Q-Type
        AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT32).serialize(new AInt32(putCallTag), dos);		//TODO Use a cleaner way to get code for GET
        tb.addFieldEndOffset();
        	//Adding Keys
        for(int i=0; i<keyFields.length; i++){
        	keySerDes[i].serialize(keyFields[i], dos);
            tb.addFieldEndOffset();
        }
        
        valueParser.parseOneRecord(new ByteArrayInputStream(admQuery.getBytes())/*new ValueStream(admQuery)*/, tb, dos);	//TODO Resue the already serialized from above
        op.addTuples(tb.getFieldEndOffsets(),tb.getByteArray(), 0, tb.getSize());
        System.out.println("Put call added to tuples in callParser in partition "+partitionId);
	}
	
	public int getPartitionId(){
		return partitionId;
	}
	
	
	private ISerializerDeserializer[] getKeySerDeser(){				//(Check AObjectSerializerDeserializer) - May want redesign (Not Sure)
		ISerializerDeserializer[] serde = new ISerializerDeserializer[keyType.length];
		int i = 0;
		for(IAType k : keyType){
			serde[i] = AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(k);
			i++;
		}
		return serde;
	}
	
	private String getLiteratForField(String fieldName, List<Pair<String, String>> params){
		for(Pair<String, String> p : params){
			if(p.first != null && p.first.equals(fieldName)){
				return p.second;
			}
		}
		return null;
	}
	
	private IAObject parseLiteral(String paramValue, IAType type) throws Exception{	//TODO Revise the exception
		switch(type.getTypeTag()){
		case INT64:
			return (new AInt64( Long.parseLong(paramValue)) );
		case INT32:
			return (new AInt32( Integer.parseInt(paramValue) ));
		case INT16:
			return (new AInt16( Short.parseShort(paramValue)));
		case INT8:
			return (new AInt8( Byte.parseByte(paramValue) ) );
		case STRING:
			return (new AString(paramValue) );
		case BOOLEAN:
			return (Boolean.parseBoolean(paramValue) ? ABoolean.TRUE : ABoolean.FALSE);
		case DOUBLE:
			return (new ADouble( Double.parseDouble(paramValue)) );
		case FLOAT:
			return (new AFloat( Float.parseFloat(paramValue)) );
		}
		throw new IllegalStateException("Invalid/Unsupported Type for key "+type.getTypeTag());
	}
	
	
	private ARecord extractRecord(ArrayTupleBuilder tb) throws HyracksDataException{
		ByteBuffer frame = ctx.allocateFrame();
        FrameTupleAppender appender = new FrameTupleAppender(ctx.getFrameSize());
        appender.reset(frame, true);
        if ( !appender.append(tb.getFieldEndOffsets(),tb.getByteArray(), 0, tb.getSize()) ) {
        	System.err.println(">>>>>> !!!!!! PROBLEM IN APPENDING ");
        }
        FrameTupleAccessor accessor = new FrameTupleAccessor(ctx.getFrameSize(), onlyValueRecDesc);
        FrameUtils.makeReadable(frame);
        accessor.reset(frame);
        FrameTupleReference tupRef = new FrameTupleReference();
        tupRef.reset(accessor, 0);
        
        
        int fieldStart = tupRef.getFieldStart(0);
        int fieldLength = tupRef.getFieldLength(0);
        ByteArrayInputStream inStream = new ByteArrayInputStream(accessor.getBuffer().array(), fieldStart, fieldLength);
        DataInput dataIn = new DataInputStream(inStream);
     	ISerializerDeserializer recSerde = AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(record);
		ARecord r = (ARecord) recSerde.deserialize( dataIn );
        return r;
	}
	
	public String toString(){
		return "KVCallParser-"+partitionId;
	}
}

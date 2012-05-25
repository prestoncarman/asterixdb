package edu.uci.ics.asterix.kvs;

import java.util.List;

import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AInt64SerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ANullSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ARecordSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AStringSerializerDeserializer;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.formats.nontagged.AqlTypeTraitProvider;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;

public class KVRequestDispatcherOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {
	
	private static final long serialVersionUID = 1L;
	final IAType[] keyType;
	final List<String> partitionKeys;
	final KVServiceID sId;
	final ARecordType record;
	final long flushPeriod;
	final int flushSize;
	
	public KVRequestDispatcherOperatorDescriptor(JobSpecification spec, IAType[] keyType, String dvName, String dsName, ARecordType record, List<String> partitionKeys, long flushPeriod, int flushSize) {
		super(spec, 0, 1);
		this.keyType = keyType;
		this.partitionKeys = partitionKeys;
		this.record = record;
		this.sId = new KVServiceID(dvName, dsName);
		this.flushPeriod = flushPeriod;
		this.flushSize = flushSize;
		try {
			recordDescriptors[0] = computeKVSQueryRecordDesc();
		} catch (HyracksDataException e) {
			System.err.print(e);
			e.printStackTrace();
		}
	}

	@Override
	public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
										IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
		
		return new KVRequestDispatcherOperatorNodePushable(ctx, partition, keyType, sId, record, partitionKeys, flushPeriod, flushSize, getOnlyValueRecordDescriptor());
	}
	
	
	private RecordDescriptor computeKVSQueryRecordDesc() throws HyracksDataException{
			//Creating SerDe
		ISerializerDeserializer[] keySerde = getKeySerDeser();
		
		ISerializerDeserializer[] serde = new ISerializerDeserializer[3 + keyType.length + 1];
		serde[0] = AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT32);
		serde[1] = AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT32);
		serde[2] = AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT32);
		
		int i = 3;
		for(ISerializerDeserializer s : keySerde){
			serde[i] = s;
			i++;
		}
		
		serde[serde.length-1] = getValueSerDeser();
		
		
			//Creating Type
		ITypeTraits[] keyTt = getKeyTypeTraits();
		ITypeTraits[] tt = new ITypeTraits[3 + keyType.length + 1];
		tt[0] = AqlTypeTraitProvider.INSTANCE.getTypeTrait(BuiltinType.AINT32);
		tt[1] = AqlTypeTraitProvider.INSTANCE.getTypeTrait(BuiltinType.AINT32);
		tt[2] = AqlTypeTraitProvider.INSTANCE.getTypeTrait(BuiltinType.AINT32);
		
		int j=3;
		for(ITypeTraits t : keyTt){
			tt[j++] = t;
		}
		
		tt[tt.length-1] = getValueTypeTraits();
		return new RecordDescriptor(serde, tt);
	}
	
	private ISerializerDeserializer[] getKeySerDeser() throws HyracksDataException{				
		ISerializerDeserializer[] serde = new ISerializerDeserializer[keyType.length];
		int i=0;
		for(IAType k : keyType){
			serde[i] = AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(k);	
			i++;
		}
		return serde;
	}
	
	private ISerializerDeserializer getValueSerDeser() throws HyracksDataException{
		return AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(record);
	}
	
	private ITypeTraits[] getKeyTypeTraits() throws HyracksDataException{				
		ITypeTraits[] tt = new ITypeTraits[keyType.length];
		int i=0;
		for(IAType k : keyType){
			tt[i] = AqlTypeTraitProvider.INSTANCE.getTypeTrait(k);	
			i++;
		}
		return tt;
	}
	
	private ITypeTraits getValueTypeTraits() {
		return AqlTypeTraitProvider.INSTANCE.getTypeTrait(record);
	}
	
		//Implemented to be used in callParser, to extract keys from adm object of put call
	private RecordDescriptor getOnlyValueRecordDescriptor() throws HyracksDataException{
		ISerializerDeserializer[] serde = new ISerializerDeserializer[] { getValueSerDeser() };
		ITypeTraits tt[] = new ITypeTraits[] { getValueTypeTraits() };
		return new RecordDescriptor(serde, tt);
	}
}
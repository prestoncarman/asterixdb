package edu.uci.ics.asterix.kvs;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.dataflow.AbstractTreeIndexOperatorDescriptor;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndex;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexRegistryProvider;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallbackProvider;

import edu.uci.ics.hyracks.storage.common.IStorageManagerInterface;

public class KVRequestHandlerOperatorDescriptor extends AbstractTreeIndexOperatorDescriptor{
	
	private static final long serialVersionUID = 1L;
	private final int numOfKeys;
	private final long flushPeriod;
	private final int flushSize;
	
	public KVRequestHandlerOperatorDescriptor(JobSpecification spec, RecordDescriptor recDesc,
			IStorageManagerInterface storageManager, IIndexRegistryProvider<IIndex> indexRegistryProvider,
            IFileSplitProvider fileSplitProvider, ITypeTraits[] typeTraits,
            IBinaryComparatorFactory[] comparatorFactories, IIndexDataflowHelperFactory dataflowHelperFactory, int numOfKeys, long flushPeriod, int flushSize) {
		
		super(spec, 1, 1, recDesc, storageManager, indexRegistryProvider, fileSplitProvider, typeTraits, comparatorFactories, dataflowHelperFactory, NoOpOperationCallbackProvider.INSTANCE);
		this.numOfKeys = numOfKeys;
		this.flushPeriod = flushPeriod;
		this.flushSize = flushSize;
	}
	
	@Override
	public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx, IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
		
		return new KVRequestHandlerOperatorNodePushable(this, ctx, partition, recordDescProvider, numOfKeys, flushPeriod, flushSize);
	}

}
package edu.uci.ics.asterix.kvs;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;

public class KVSResponseDispatcherOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

	private static final long serialVersionUID = 1L;
	
	public KVSResponseDispatcherOperatorDescriptor(JobSpecification spec) {
		super(spec, 1, 0);
		
	}

	@Override
	public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx, 
									IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
		
		return new KVSResponseDispatcherOperatorNodePushable(ctx, this, recordDescProvider);
	}

}
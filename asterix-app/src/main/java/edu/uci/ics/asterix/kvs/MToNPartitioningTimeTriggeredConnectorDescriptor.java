package edu.uci.ics.asterix.kvs;

import java.util.BitSet;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.comm.IPartitionCollector;
import edu.uci.ics.hyracks.api.comm.IPartitionWriterFactory;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractMToNConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.collectors.NonDeterministicChannelReader;
import edu.uci.ics.hyracks.dataflow.std.collectors.NonDeterministicFrameReader;
import edu.uci.ics.hyracks.dataflow.std.collectors.PartitionCollector;

public class MToNPartitioningTimeTriggeredConnectorDescriptor extends AbstractMToNConnectorDescriptor {
	
	private static final long serialVersionUID = 1L;
	private ITuplePartitionComputerFactory tpcf;
	private final long flushPeriod;
	private final int flushSize;
	
	public MToNPartitioningTimeTriggeredConnectorDescriptor(
			JobSpecification spec, ITuplePartitionComputerFactory tpcf, long flushPeriod, int flushSize) {
		super(spec);
		this.tpcf = tpcf;
		this.flushPeriod = flushPeriod;
		this.flushSize = flushSize;
	}
	
	@Override
    public IFrameWriter createPartitioner(IHyracksTaskContext ctx, RecordDescriptor recordDesc,
            IPartitionWriterFactory edwFactory, int index, int nProducerPartitions, int nConsumerPartitions)
            throws HyracksDataException {
        final TimeTriggeredPartitionDataWriter hashWriter = new TimeTriggeredPartitionDataWriter(ctx, nConsumerPartitions, edwFactory,
                recordDesc, tpcf.createPartitioner(), flushPeriod, flushSize);
        return hashWriter;
    }

	@Override
    public IPartitionCollector createPartitionCollector(IHyracksTaskContext ctx, RecordDescriptor recordDesc,
            int index, int nProducerPartitions, int nConsumerPartitions) throws HyracksDataException {
        BitSet expectedPartitions = new BitSet(nProducerPartitions);
        expectedPartitions.set(0, nProducerPartitions);
        NonDeterministicChannelReader channelReader = new NonDeterministicChannelReader(nProducerPartitions,
                expectedPartitions);
        NonDeterministicFrameReader frameReader = new NonDeterministicFrameReader(channelReader);
        return new PartitionCollector(ctx, getConnectorId(), index, expectedPartitions, frameReader, channelReader);
    }

}

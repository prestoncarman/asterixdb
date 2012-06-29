package edu.uci.ics.asterix.external.dataset.adapter;

import java.nio.ByteBuffer;
import java.util.Map;

import edu.uci.ics.asterix.feed.intake.IPullBasedFeedClient;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;

public abstract class PullBasedAdapter extends AbstractDatasourceAdapter implements IDatasourceAdapter {

    protected ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(1);
    protected IPullBasedFeedClient pullBasedFeedClient;
    private FrameTupleAppender appender;
    private ByteBuffer frame;

    public abstract IPullBasedFeedClient getFeedClient(int partition) throws Exception;

    @Override
    public abstract AdapterDataFlowType getAdapterDataFlowType();

    @Override
    public abstract AdapterType getAdapterType();

    @Override
    public abstract void configure(Map<String, String> arguments) throws Exception;

    @Override
    public abstract IAType getAdapterOutputType();

    @Override
    public abstract void initialize(IHyracksTaskContext ctx) throws Exception;

    @Override
    public void start(int partition, IFrameWriter writer) throws Exception {
        appender = new FrameTupleAppender(ctx.getFrameSize());
        frame = ctx.allocateFrame();
        appender.reset(frame, true);

        boolean newData = false;
        pullBasedFeedClient = getFeedClient(partition);
        while (true) {
            tupleBuilder.reset();
            newData = pullBasedFeedClient.nextTuple(tupleBuilder.getDataOutput()); //nextTuple is a blocking call.
            if (newData) {
                tupleBuilder.addFieldEndOffset();
                appendTupleToFrame(writer);
            }
        }
    }

    private void appendTupleToFrame(IFrameWriter writer) throws HyracksDataException {
        if (!appender.append(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray(), 0, tupleBuilder.getSize())) {
            FrameUtils.flushFrame(frame, writer);
            appender.reset(frame, true);
            if (!appender.append(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray(), 0,
                    tupleBuilder.getSize())) {
                throw new IllegalStateException();
            }
        }
    }

}

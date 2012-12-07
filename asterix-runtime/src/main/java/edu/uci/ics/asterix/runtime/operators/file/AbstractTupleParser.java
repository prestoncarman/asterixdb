package edu.uci.ics.asterix.runtime.operators.file;

import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParser;

public abstract class AbstractTupleParser implements ITupleParser {

    protected ArrayTupleBuilder tb = new ArrayTupleBuilder(1);
    protected DataOutput dos = tb.getDataOutput();
    protected final FrameTupleAppender appender;
    protected final ByteBuffer frame;
    protected final ARecordType recType;
    protected final IHyracksTaskContext ctx;

    public AbstractTupleParser(IHyracksTaskContext ctx, ARecordType recType) {
        appender = new FrameTupleAppender(ctx.getFrameSize());
        frame = ctx.allocateFrame();
        this.recType = recType;
        this.ctx = ctx;
    }

    public abstract IDataParser getDataParser();

    @Override
    public void parse(InputStream in, IFrameWriter writer) throws HyracksDataException {

        appender.reset(frame, true);
        IDataParser parser = getDataParser();
        try {
            parser.initialize(in, recType, true);
            while (true) {
                tb.reset();
                if (!parser.parse(tb.getDataOutput())) {
                    break;
                }
                tb.addFieldEndOffset();
                addTupleToFrame(writer);
            }
            if (appender.getTupleCount() > 0) {
                FrameUtils.flushFrame(frame, writer);
            }
        } catch (AsterixException ae) {
            throw new HyracksDataException(ae);
        } catch (IOException ioe) {
            throw new HyracksDataException(ioe);
        }
    }

    protected void addTupleToFrame(IFrameWriter writer) throws HyracksDataException {
        if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
            FrameUtils.flushFrame(frame, writer);
            appender.reset(frame, true);
            if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                throw new IllegalStateException();
            }
        }

    }

}

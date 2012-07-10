package edu.uci.ics.asterix.runtime.operators.file;

import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IValueParserFactory;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParser;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParserFactory;

public class NtDelimitedDataTupleParserFactory implements ITupleParserFactory {
	private static final long serialVersionUID = 1L;
	protected ARecordType recordType;
	protected IValueParserFactory[] valueParserFactories;
	protected char fieldDelimiter;

	public NtDelimitedDataTupleParserFactory(ARecordType recordType,
			IValueParserFactory[] valueParserFactories, char fieldDelimiter) {
		this.recordType = recordType;
		this.valueParserFactories = valueParserFactories;
		this.fieldDelimiter = fieldDelimiter;
	}

	@Override
	public ITupleParser createTupleParser(final IHyracksTaskContext ctx) {
		return new DelimitedDataTupleParser(ctx, recordType,
				valueParserFactories, fieldDelimiter);
	}

}

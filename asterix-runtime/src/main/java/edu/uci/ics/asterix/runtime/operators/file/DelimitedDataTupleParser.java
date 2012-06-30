package edu.uci.ics.asterix.runtime.operators.file;

import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IValueParserFactory;

public class DelimitedDataTupleParser extends AbstractTupleParser {

	private final DelimitedDataParser dataParser;

	public DelimitedDataTupleParser(IHyracksTaskContext ctx,
			ARecordType recType, IValueParserFactory[] valueParserFactories,
			char fieldDelimter) {
		super(ctx, recType);
		dataParser = new DelimitedDataParser(recType, valueParserFactories,
				fieldDelimter);
	}

	public IDataParser getDataParser() {
		return dataParser;
	}

}

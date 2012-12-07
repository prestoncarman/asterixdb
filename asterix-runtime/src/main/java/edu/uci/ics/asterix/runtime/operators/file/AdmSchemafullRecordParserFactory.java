package edu.uci.ics.asterix.runtime.operators.file;

import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParser;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParserFactory;

public class AdmSchemafullRecordParserFactory implements ITupleParserFactory {

	private static final long serialVersionUID = 1L;

	protected ARecordType recType;

	public AdmSchemafullRecordParserFactory(ARecordType recType) {
		this.recType = recType;
	}

	@Override
	public ITupleParser createTupleParser(final IHyracksTaskContext ctx) {
		return new AdmTupleParser(ctx, recType);
	}

}
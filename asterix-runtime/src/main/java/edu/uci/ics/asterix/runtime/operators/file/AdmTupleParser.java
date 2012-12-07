package edu.uci.ics.asterix.runtime.operators.file;

import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;

public class AdmTupleParser extends AbstractTupleParser {

    public AdmTupleParser(IHyracksTaskContext ctx, ARecordType recType) {
        super(ctx, recType);
    }

    @Override
    public IDataParser getDataParser() {
        return new ADMDataParser();
    }

}

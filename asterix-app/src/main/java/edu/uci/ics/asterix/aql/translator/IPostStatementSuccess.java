package edu.uci.ics.asterix.aql.translator;

import edu.uci.ics.asterix.metadata.MetadataTransactionContext;

public interface IPostStatementSuccess {

    public void doPostSuccess(MetadataTransactionContext mdTxnCtx) throws Exception;
}

package edu.uci.ics.asterix.aql.translator;

import edu.uci.ics.asterix.metadata.MetadataTransactionContext;

public interface IPostStatementFailure {

    public void doPostFailure(MetadataTransactionContext mdTxnCtx) throws Exception;

}

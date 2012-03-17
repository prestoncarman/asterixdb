package edu.uci.ics.asterix.common.context;

import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionProvider;


public interface INodeApplicationState {
    public TransactionProvider getTransactionProvider();
    
    public void setTransactionProvider(TransactionProvider provider);
    
    public AsterixAppRuntimeContext getApplicationRuntimeContext();
    
    public void setApplicationRuntimeContext(AsterixAppRuntimeContext context);

}

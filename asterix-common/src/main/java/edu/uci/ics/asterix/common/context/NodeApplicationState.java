package edu.uci.ics.asterix.common.context;

import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionProvider;

public class NodeApplicationState implements INodeApplicationState {

    private AsterixAppRuntimeContext context;
    private TransactionProvider provider;

    @Override
    public TransactionProvider getTransactionProvider() {
        return provider;
    }

    @Override
    public void setTransactionProvider(TransactionProvider provider) {
        this.provider = provider;
    }

    @Override
    public AsterixAppRuntimeContext getApplicationRuntimeContext() {
        return context;
    }

    @Override
    public void setApplicationRuntimeContext(AsterixAppRuntimeContext context) {
        this.context = context;
    }
}

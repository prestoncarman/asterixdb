package edu.uci.ics.asterix.external.adapter.factory;

import java.util.Map;

import edu.uci.ics.asterix.external.dataset.adapter.CNNFeedAdapter;
import edu.uci.ics.asterix.external.dataset.adapter.IDatasourceAdapter;

public class CNNFeedAdapterFactory implements ITypedDatasourceAdapterFactory {

    @Override
    public IDatasourceAdapter createAdapter(Map<String, String> configuration) throws Exception {
        CNNFeedAdapter cnnFeedAdapter = new CNNFeedAdapter();
        cnnFeedAdapter.configure(configuration);
        return cnnFeedAdapter;
    }

    @Override
    public AdapterType getAdapterType() {
        return AdapterType.TYPED;
    }

}

package edu.uci.ics.asterix.external.adapter.factory;

import java.util.Map;

import edu.uci.ics.asterix.external.dataset.adapter.IDatasourceAdapter;
import edu.uci.ics.asterix.external.dataset.adapter.PullBasedTwitterAdapter;

public class PullBasedTwitterAdapterFactory implements ITypedDatasourceAdapterFactory {

    @Override
    public IDatasourceAdapter createAdapter(Map<String, String> configuration) throws Exception {
        PullBasedTwitterAdapter twitterAdapter = new PullBasedTwitterAdapter();
        twitterAdapter.configure(configuration);
        return twitterAdapter;
    }

    @Override
    public AdapterType getAdapterType() {
        return AdapterType.TYPED;
    }
}

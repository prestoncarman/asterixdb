package edu.uci.ics.asterix.external.adapter.factory;

import java.util.Map;

import edu.uci.ics.asterix.external.dataset.adapter.IDatasourceAdapter;
import edu.uci.ics.asterix.external.dataset.adapter.RSSFeedAdapter;

public class RSSFeedAdapterFactory implements ITypedDatasetAdapterFactory {

    @Override
    public IDatasourceAdapter createAdapter(Map<String, String> configuration) throws Exception {
        RSSFeedAdapter rssFeedAdapter = new RSSFeedAdapter();
        rssFeedAdapter.configure(configuration);
        return rssFeedAdapter;
    }

    @Override
    public String getName() {
        return "rss_feed";
    }

}

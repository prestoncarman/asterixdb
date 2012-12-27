package edu.uci.ics.asterix.external.adapter.factory;

import java.util.Map;

import edu.uci.ics.asterix.external.dataset.adapter.HDFSAdapter;
import edu.uci.ics.asterix.external.dataset.adapter.IDatasourceAdapter;
import edu.uci.ics.asterix.om.types.IAType;

public class HDFSAdapterFactory implements IGenericDatasetAdapterFactory {

    @Override
    public IDatasourceAdapter createAdapter(Map<String, String> configuration, IAType atype) throws Exception {
        HDFSAdapter hdfsAdapter = new HDFSAdapter(atype);
        hdfsAdapter.configure(configuration);
        return hdfsAdapter;
    }

    @Override
    public String getName() {
        return "hdfs";
    }

}

package edu.uci.ics.asterix.external.dataset.adapter;

import edu.uci.ics.asterix.om.types.ARecordType;

public interface ITypedDatasourceAdapter extends IDatasourceAdapter {

    public ARecordType getAdapterOutputType();

}

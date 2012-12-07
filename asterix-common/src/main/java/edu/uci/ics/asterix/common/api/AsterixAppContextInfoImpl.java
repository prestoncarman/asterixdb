package edu.uci.ics.asterix.common.api;

import edu.uci.ics.asterix.common.context.AsterixIndexRegistryProvider;
import edu.uci.ics.asterix.common.context.AsterixStorageManagerInterface;
import edu.uci.ics.asterix.common.dataflow.IAsterixApplicationContextInfo;
import edu.uci.ics.hyracks.api.application.ICCApplicationContext;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndex;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexRegistryProvider;
import edu.uci.ics.hyracks.storage.common.IStorageManagerInterface;

public class AsterixAppContextInfoImpl implements IAsterixApplicationContextInfo {

    private static AsterixAppContextInfoImpl INSTANCE;

    private final ICCApplicationContext appCtx;

    public static void initialize(ICCApplicationContext ccAppCtx) {
        if (INSTANCE == null) {
            INSTANCE = new AsterixAppContextInfoImpl(ccAppCtx);
        }
    }

    private AsterixAppContextInfoImpl(ICCApplicationContext ccAppCtx) {
        this.appCtx = ccAppCtx;
    }

    public static IAsterixApplicationContextInfo getInstance() {
        return INSTANCE;
    }

    @Override
    public IIndexRegistryProvider<IIndex> getIndexRegistryProvider() {
        return AsterixIndexRegistryProvider.INSTANCE;
    }

    @Override
    public IStorageManagerInterface getStorageManagerInterface() {
        return AsterixStorageManagerInterface.INSTANCE;
    }

    @Override
    public ICCApplicationContext getCCApplicationContext() {
        return appCtx;
    }

}

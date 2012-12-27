package edu.uci.ics.asterix.external.dataset.adapter;

import java.io.DataOutput;

import edu.uci.ics.asterix.common.exceptions.AsterixException;

public interface IPullBasedFeedClient {

    public boolean nextTuple(DataOutput dataOutput) throws AsterixException;

    public void resetOnFailure(Exception e) throws AsterixException;

    public void stop() throws Exception;

}

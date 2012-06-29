package edu.uci.ics.asterix.feed.intake;

import java.io.DataOutput;

import edu.uci.ics.asterix.common.exceptions.AsterixException;

public interface IPullBasedFeedClient {

    public enum status {
        MORE_DATA,
        END_OF_DATA
    }
    
    public boolean nextTuple(DataOutput dataOutput) throws AsterixException;

}

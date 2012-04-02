package edu.uci.ics.asterix.dataflow.data.common;

import edu.uci.ics.hyracks.storage.am.invertedindex.tokenizers.IBinaryTokenizerFactory;

public interface IBinaryTokenizerFactoryProvider {
    public IBinaryTokenizerFactory getWordTokenizerFactory(Object type, boolean hashedTokens);
    public IBinaryTokenizerFactory getNGramTokenizerFactory(Object type, int gramLength, boolean usePrePost, boolean hashedTokens);
}

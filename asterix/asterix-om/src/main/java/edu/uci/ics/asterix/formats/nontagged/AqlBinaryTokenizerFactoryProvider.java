package edu.uci.ics.asterix.formats.nontagged;

import edu.uci.ics.asterix.dataflow.data.common.IBinaryTokenizerFactoryProvider;
import edu.uci.ics.asterix.dataflow.data.common.ListElementTokenFactory;
import edu.uci.ics.asterix.dataflow.data.common.OrderedListBinaryTokenizerFactory;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.hyracks.storage.am.invertedindex.tokenizers.DelimitedUTF8StringBinaryTokenizerFactory;
import edu.uci.ics.hyracks.storage.am.invertedindex.tokenizers.HashedUTF8WordTokenFactory;
import edu.uci.ics.hyracks.storage.am.invertedindex.tokenizers.IBinaryTokenizerFactory;
import edu.uci.ics.hyracks.storage.am.invertedindex.tokenizers.UTF8NGramTokenFactory;
import edu.uci.ics.hyracks.storage.am.invertedindex.tokenizers.UTF8WordTokenFactory;

public class AqlBinaryTokenizerFactoryProvider implements IBinaryTokenizerFactoryProvider {

    public static final AqlBinaryTokenizerFactoryProvider INSTANCE = new AqlBinaryTokenizerFactoryProvider();

    private static final IBinaryTokenizerFactory aqlStringTokenizer = new DelimitedUTF8StringBinaryTokenizerFactory(
            true, true, new UTF8WordTokenFactory(ATypeTag.STRING.serialize(), ATypeTag.INT32.serialize()));
    
    private static final IBinaryTokenizerFactory aqlHashingStringTokenizer = new DelimitedUTF8StringBinaryTokenizerFactory(
            true, true, new HashedUTF8WordTokenFactory(ATypeTag.INT32.serialize(), ATypeTag.INT32.serialize()));

    private static final IBinaryTokenizerFactory orderedListTokenizer = new OrderedListBinaryTokenizerFactory(new ListElementTokenFactory());
    
    @Override
    public IBinaryTokenizerFactory getWordTokenizerFactory(ATypeTag typeTag, boolean hashedTokens) {
        switch (typeTag) {
            case STRING: {
                if (hashedTokens) {
                    return aqlHashingStringTokenizer;
                } else {
                    return aqlStringTokenizer;
                }
            }
            case ORDEREDLIST: {
                return orderedListTokenizer;
            }
            default: {
                return null;
            }
        }
    }

    @Override
    public IBinaryTokenizerFactory getNGramTokenizerFactory(ATypeTag typeTag, int gramLength, boolean usePrePost,
            boolean hashedTokens) {
        switch (typeTag) {
            case STRING: {
                if (hashedTokens) {
                    return null;
                } else {
                    return new NGramUTF8StringBinaryTokenizerFactory(gramLength, usePrePost, true, true,
                            new UTF8NGramTokenFactory(ATypeTag.STRING.serialize(), ATypeTag.INT32.serialize()));
                }
            }
            case ORDEREDLIST: {
                return orderedListTokenizer;
            }
            default: {
                return null;
            }
        }
    }
}

package edu.uci.ics.asterix.dataflow.data.common;

import edu.uci.ics.hyracks.storage.am.invertedindex.tokenizers.IBinaryTokenizer;
import edu.uci.ics.hyracks.storage.am.invertedindex.tokenizers.IBinaryTokenizerFactory;
import edu.uci.ics.hyracks.storage.am.invertedindex.tokenizers.ITokenFactory;

public class OrderedListBinaryTokenizerFactory implements IBinaryTokenizerFactory {

    private static final long serialVersionUID = 1L;
    private final ITokenFactory tokenFactory;
    
    public OrderedListBinaryTokenizerFactory(ITokenFactory tokenFactory) {
        this.tokenFactory = tokenFactory;
    }
    
    @Override
    public IBinaryTokenizer createTokenizer() {
        return new OrderedListBinaryTokenizer(tokenFactory);
    }
}

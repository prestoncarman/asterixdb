package edu.uci.ics.asterix.lexergenerator.rulegenerators;

import edu.uci.ics.asterix.lexergenerator.LexerNode;

public interface RuleGenerator {
    public LexerNode generate(String input) throws Exception;
}

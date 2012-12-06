package com.ogeidix.lexergenerator.rulegenerators;

import com.ogeidix.lexergenerator.LexerNode;

public interface RuleGenerator {
    public LexerNode generate(String input) throws Exception;
}

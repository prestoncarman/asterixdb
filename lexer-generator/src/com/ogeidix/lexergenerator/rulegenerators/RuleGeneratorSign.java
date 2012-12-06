package com.ogeidix.lexergenerator.rulegenerators;

import com.ogeidix.lexergenerator.LexerNode;
import com.ogeidix.lexergenerator.rules.RuleChar;

public class RuleGeneratorSign implements RuleGenerator {

    @Override
    public LexerNode generate(String input) throws Exception {
        LexerNode result = new LexerNode();
        result.add(new RuleChar('+'));
        result.add(new RuleChar('-'));
        return result;
    }

}

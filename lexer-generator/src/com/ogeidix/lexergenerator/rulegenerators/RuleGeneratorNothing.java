package com.ogeidix.lexergenerator.rulegenerators;

import com.ogeidix.lexergenerator.LexerNode;
import com.ogeidix.lexergenerator.rules.RuleEpsilon;

public class RuleGeneratorNothing implements RuleGenerator {

    @Override
    public LexerNode generate(String input) throws Exception {
        LexerNode node =  new LexerNode();
        node.add(new RuleEpsilon());
        return node;
    }

}

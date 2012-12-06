package com.ogeidix.lexergenerator.rulegenerators;

import com.ogeidix.lexergenerator.LexerNode;
import com.ogeidix.lexergenerator.rules.RulePartial;

public class RuleGeneratorToken implements RuleGenerator {

    @Override
    public LexerNode generate(String input) throws Exception {
        if (input==null || input.length() == 0) throw new Exception("Wrong rule format for generator token : " + input);
        LexerNode node =  new LexerNode();
        node.add(new RulePartial(input));
        return node;
    }

}

package com.ogeidix.lexergenerator.rulegenerators;

import com.ogeidix.lexergenerator.LexerNode;
import com.ogeidix.lexergenerator.rules.RuleChar;

public class RuleGeneratorString implements RuleGenerator {

    @Override
    public LexerNode generate(String input) {
        LexerNode result = new LexerNode();
        if (input == null) return result; 
        for (int i = 0; i < input.length(); i++) {
            result.append(new RuleChar(input.charAt(i)));
        }
        return result;
    }

}

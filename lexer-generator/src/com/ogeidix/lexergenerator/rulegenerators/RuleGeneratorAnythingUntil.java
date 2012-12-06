package com.ogeidix.lexergenerator.rulegenerators;

import com.ogeidix.lexergenerator.LexerNode;
import com.ogeidix.lexergenerator.rules.RuleAnythingUntil;

public class RuleGeneratorAnythingUntil implements RuleGenerator {

    @Override
    public LexerNode generate(String input) throws Exception {
        LexerNode result = new LexerNode();
        if (input == null || input.length()!=1) throw new Exception("Wrong rule format for generator anythingExcept: " + input); 
        result.append(new RuleAnythingUntil(input.charAt(0)));
        return result;
    }

}

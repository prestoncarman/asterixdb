package com.ogeidix.lexergenerator.rulegenerators;

import com.ogeidix.lexergenerator.LexerNode;
import com.ogeidix.lexergenerator.rules.RuleDigitSequence;

public class RuleGeneratorDigitSequence implements RuleGenerator {

    @Override
    public LexerNode generate(String input) throws Exception {
        LexerNode result = new LexerNode(); 
        result.append(new RuleDigitSequence());
        return result;
    }

}

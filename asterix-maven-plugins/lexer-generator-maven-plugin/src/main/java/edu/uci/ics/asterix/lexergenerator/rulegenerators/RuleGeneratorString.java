package edu.uci.ics.asterix.lexergenerator.rulegenerators;


import edu.uci.ics.asterix.lexergenerator.LexerNode;
import edu.uci.ics.asterix.lexergenerator.rules.RuleChar;

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

package edu.uci.ics.asterix.lexergenerator.rulegenerators;


import edu.uci.ics.asterix.lexergenerator.LexerNode;
import edu.uci.ics.asterix.lexergenerator.rules.RuleDigitSequence;

public class RuleGeneratorDigitSequence implements RuleGenerator {

    @Override
    public LexerNode generate(String input) throws Exception {
        LexerNode result = new LexerNode(); 
        result.append(new RuleDigitSequence());
        return result;
    }

}

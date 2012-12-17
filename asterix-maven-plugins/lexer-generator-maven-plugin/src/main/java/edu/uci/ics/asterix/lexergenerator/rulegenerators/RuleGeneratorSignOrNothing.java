package edu.uci.ics.asterix.lexergenerator.rulegenerators;


import edu.uci.ics.asterix.lexergenerator.LexerNode;
import edu.uci.ics.asterix.lexergenerator.rules.RuleChar;
import edu.uci.ics.asterix.lexergenerator.rules.RuleEpsilon;

public class RuleGeneratorSignOrNothing implements RuleGenerator {

    @Override
    public LexerNode generate(String input) throws Exception {
        LexerNode result = new LexerNode();
        result.add(new RuleChar('+'));
        result.add(new RuleChar('-'));
        result.add(new RuleEpsilon());
        return result;
    }

}

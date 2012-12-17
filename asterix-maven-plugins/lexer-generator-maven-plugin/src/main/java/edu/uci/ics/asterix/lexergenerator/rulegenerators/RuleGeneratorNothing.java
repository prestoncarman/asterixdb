package edu.uci.ics.asterix.lexergenerator.rulegenerators;


import edu.uci.ics.asterix.lexergenerator.LexerNode;
import edu.uci.ics.asterix.lexergenerator.rules.RuleEpsilon;

public class RuleGeneratorNothing implements RuleGenerator {

    @Override
    public LexerNode generate(String input) throws Exception {
        LexerNode node =  new LexerNode();
        node.add(new RuleEpsilon());
        return node;
    }

}

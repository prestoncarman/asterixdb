package edu.uci.ics.asterix.lexergenerator.rulegenerators;


import edu.uci.ics.asterix.lexergenerator.LexerNode;
import edu.uci.ics.asterix.lexergenerator.rules.RulePartial;

public class RuleGeneratorToken implements RuleGenerator {

    @Override
    public LexerNode generate(String input) throws Exception {
        if (input==null || input.length() == 0) throw new Exception("Wrong rule format for generator token : " + input);
        LexerNode node =  new LexerNode();
        node.add(new RulePartial(input));
        return node;
    }

}

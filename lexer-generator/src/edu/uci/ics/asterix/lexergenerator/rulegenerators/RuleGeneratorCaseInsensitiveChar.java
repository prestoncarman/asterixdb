package edu.uci.ics.asterix.lexergenerator.rulegenerators;


import edu.uci.ics.asterix.lexergenerator.LexerNode;
import edu.uci.ics.asterix.lexergenerator.rules.RuleChar;

public class RuleGeneratorCaseInsensitiveChar implements RuleGenerator {

    @Override
    public LexerNode generate(String input) throws Exception {
        LexerNode result = new LexerNode();
        if (input == null || input.length()!=1) throw new Exception("Wrong rule format for generator char: " + input);
        char cl = Character.toLowerCase(input.charAt(0));
        char cu = Character.toUpperCase(cl);
        result.add(new RuleChar(cl));
        result.add(new RuleChar(cu));
        return result;
    }

}

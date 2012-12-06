package edu.uci.ics.asterix.lexergenerator;

import java.util.HashMap;

import edu.uci.ics.asterix.lexergenerator.rulegenerators.*;

public class NodeChainFactory {
    static private HashMap<String, RuleGenerator> ruleGenerators = new HashMap<String, RuleGenerator>();

    static {
        ruleGenerators.put("char",                new RuleGeneratorChar());
        ruleGenerators.put("string",              new RuleGeneratorString());
        ruleGenerators.put("anythingUntil",       new RuleGeneratorAnythingUntil());
        ruleGenerators.put("signOrNothing",       new RuleGeneratorSignOrNothing());
        ruleGenerators.put("sign",                new RuleGeneratorSign());
        ruleGenerators.put("digitSequence",       new RuleGeneratorDigitSequence());
        ruleGenerators.put("caseInsensitiveChar", new RuleGeneratorCaseInsensitiveChar());
        ruleGenerators.put("charOrNothing",       new RuleGeneratorCharOrNothing());
        ruleGenerators.put("token",               new RuleGeneratorToken());
        ruleGenerators.put("nothing",             new RuleGeneratorNothing());
    }

    public static LexerNode create(String generator, String constructor) throws Exception{
        constructor = constructor.replace("@","aux_");
        if (ruleGenerators.get(generator) == null) throw new Exception("Rule Generator not found for '"+generator+"'");
        return ruleGenerators.get(generator).generate(constructor);
    }
}

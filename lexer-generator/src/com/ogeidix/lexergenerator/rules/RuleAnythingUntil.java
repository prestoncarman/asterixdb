package com.ogeidix.lexergenerator.rules;


public class RuleAnythingUntil implements Rule {
    
    private char expected;
    
    public RuleAnythingUntil clone(){
        return new RuleAnythingUntil(expected);
    }
    
    public RuleAnythingUntil(char expected){
        this.expected = expected;
    }

    @Override
    public String toString(){
        return " .* "+String.valueOf(expected);
    }

    @Override
    public int hashCode() {
        return 10*(int) expected;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) return false;
        if (o instanceof RuleAnythingUntil){
            if (((RuleAnythingUntil) o).expected == this.expected){
                return true;
            }
        }
        return false;
    }

    @Override
    public String javaAction() {
        return "currentChar = readNextChar();";
    }

    @Override
    public String javaMatch(String action) {
        StringBuilder result = new StringBuilder();
        result.append("boolean escaped = false;");
        result.append("while (currentChar!='").append(expected).append("' || escaped)");
        result.append("{\nif(!escaped && currentChar=='\\\\\\\\'){escaped=true;}\nelse {escaped=false;}\ncurrentChar = readNextChar();\n}");
        result.append("\nif (currentChar=='").append(expected).append("'){");
        result.append(action);
        result.append("}\n");
        return result.toString();
    }

}

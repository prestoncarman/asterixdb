package com.ogeidix.lexergenerator.rules;


public class RuleChar implements Rule {
    
    private char expected;
    
    public RuleChar clone(){
        return new RuleChar(expected);
    }
    
    public RuleChar(char expected){
        this.expected = expected;
    }

    @Override
    public String toString(){
        return String.valueOf(expected);
    }
    
    public char expectedChar(){
        return expected;
    }

    @Override
    public int hashCode() {
        return (int) expected;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) return false;
        if (o instanceof RuleChar){
            if (((RuleChar) o).expected == this.expected){
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
        result.append("if (currentChar=='");
        result.append(expected);
        result.append("'){");
        result.append(action);
        result.append("}");
        return result.toString();
    }
}

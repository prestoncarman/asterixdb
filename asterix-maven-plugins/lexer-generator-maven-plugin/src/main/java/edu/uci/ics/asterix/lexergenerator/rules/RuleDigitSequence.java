package edu.uci.ics.asterix.lexergenerator.rules;


public class RuleDigitSequence implements Rule {

    public RuleDigitSequence clone(){
        return new RuleDigitSequence();
    }
    
    @Override
    public String toString(){
        return " [0-9]+ ";
    }

    @Override
    public int hashCode() {
        return 1;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) return false;
        if (o instanceof RuleDigitSequence){
                return true;
        }
        return false;
    }

    @Override
    public String javaAction() {
        return "";
    }

    @Override
    public String javaMatch(String action) {
        StringBuilder result = new StringBuilder();
        result.append("if(currentChar >= '0' && currentChar<='9'){" +
        		        "\ncurrentChar = readNextChar();" +
        		        "\nwhile(currentChar >= '0' && currentChar<='9'){" +
        		            "\ncurrentChar = readNextChar();" +
        		        "\n}\n");
        result.append(action);
        result.append("\n}");
        return result.toString();
    }
}

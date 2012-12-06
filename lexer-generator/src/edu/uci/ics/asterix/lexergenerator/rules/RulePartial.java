package edu.uci.ics.asterix.lexergenerator.rules;


public class RulePartial implements Rule {
    
    private String partialName;
    
    public RulePartial clone(){
        return new RulePartial(partialName);
    }
    
    public RulePartial(String expected){
        this.partialName = expected;
    }

    public String getPartial(){
        return this.partialName;
    }
    
    @Override
    public String toString(){
        return partialName;
    }

    @Override
    public int hashCode() {
        return (int) partialName.charAt(1);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) return false;
        if (o instanceof RulePartial){
            if (((RulePartial) o).partialName.equals(this.partialName)){
                return true;
            }
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
        result.append("if (parse_"+partialName+"(currentChar)==TOKEN_"+partialName+"){");
        result.append(action);
        result.append("}");
        return result.toString();
    }
    
}

package edu.uci.ics.asterix.lexergenerator.rules;

public interface Rule {
    public int hashCode();
    public boolean equals(Object o);
    public String toString();
    
    public String javaAction();
    public String javaMatch(String action);
    public Rule   clone();
}

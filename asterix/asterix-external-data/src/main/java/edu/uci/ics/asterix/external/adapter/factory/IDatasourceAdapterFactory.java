package edu.uci.ics.asterix.external.adapter.factory;

public interface IDatasourceAdapterFactory {
    
    public enum AdapterType {
        TYPED,
        GENERIC
    }
    
    public AdapterType getAdapterType();
    
}

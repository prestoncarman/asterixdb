package edu.uci.ics.asterix.kvs;

import java.io.Serializable;


public class KVServiceID implements Serializable{
	
	private static final long serialVersionUID = 1L;
	
	private String dataverseName;
	private String datasetName;
	
	public KVServiceID(){
		this(null, null);
	}
	
	public KVServiceID(String dvName, String dsName){
		dataverseName = dvName;
		datasetName = dsName;
	}
	
	public String getDataverseName(){
		return dataverseName;
	}
	
	public String getDatasetName(){
		return datasetName;
	}
	
	public void reset(String dvName, String dsName){
		dataverseName = dvName;
		datasetName = dsName;
	}
	
	@Override
	public boolean equals(Object o){
		if (o == null || !(o instanceof KVServiceID)) {
            return false;
        }
        if (((KVServiceID) o).getDataverseName().equals(dataverseName) && ((KVServiceID) o).getDatasetName().equals(datasetName)) {
            return true;
        }
        return false;
	}
	
	@Override
	public int hashCode() {
		return dataverseName.hashCode() + datasetName.hashCode();
	}
	
	public String toString(){
		return "("+dataverseName+", "+datasetName+")";
	}
}

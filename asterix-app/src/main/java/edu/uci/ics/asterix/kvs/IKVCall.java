package edu.uci.ics.asterix.kvs;

public interface IKVCall {
	
	public enum KVCallType{
		REGISTER,
		GET,
		PUT
	}
	
	public KVCallType getType();
	
	public int getId();

}

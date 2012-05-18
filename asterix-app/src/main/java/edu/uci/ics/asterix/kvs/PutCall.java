package edu.uci.ics.asterix.kvs;

public class PutCall implements IKVCall{
	
	private int id;
	private String admRecord;
	
	public PutCall(int id, String rawAdmRec){
		this.id = id;
		admRecord = rawAdmRec;
	}
	
	public String getRecord(){
		return admRecord;
	}
	
	@Override
	public KVCallType getType() {
		return KVCallType.PUT;
	}

	@Override
	public int getId() {
		return id;
	}

}

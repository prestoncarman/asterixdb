package edu.uci.ics.asterix.kvs;

import java.util.List;

import edu.uci.ics.hyracks.algebricks.common.utils.Pair;


public class GetCall implements IKVCall{
	
	private int id;
	private List< Pair<String, String> > keys;
	
	public GetCall(int id, List< Pair<String, String> > keys){
		this.id = id;
		this.keys = keys;
	}
	
	public List< Pair<String, String> > getKeys(){
		return keys;
	}
	
	@Override
	public KVCallType getType() {
		return KVCallType.GET;
	}

	@Override
	public int getId() {
		return id;
	}

}

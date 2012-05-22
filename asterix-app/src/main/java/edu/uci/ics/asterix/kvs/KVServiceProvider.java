package edu.uci.ics.asterix.kvs;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import edu.uci.ics.asterix.om.types.ARecordType;

public class KVServiceProvider {
	public static final KVServiceProvider INSTANCE = new KVServiceProvider(); 
	private static final AtomicInteger queryIdGen = new AtomicInteger(0);
	
	private static final HashMap<KVServiceID, LinkedBlockingQueue<IKVCall> > serviceMap = new HashMap<KVServiceID, LinkedBlockingQueue<IKVCall> >();
	private static final HashMap<Integer, LinkedBlockingQueue<Object[]>> outputmap = new HashMap<Integer, LinkedBlockingQueue<Object[]>>();
	private static final HashMap<KVServiceID, ARecordType> schemaMap = new HashMap<KVServiceID, ARecordType>();
	
	private KVServiceProvider(){
	}
	
	public LinkedBlockingQueue<IKVCall> getQueryQueue(KVServiceID serviceId){
		return serviceMap.get(serviceId);
	}
	
	public LinkedBlockingQueue<Object[]> getOutputQueue(int queryId){
		return outputmap.get(queryId);
	}
	
	public int generateNextQueryId(){
		return queryIdGen.getAndIncrement();
	}
	
	public void registerQueryQueue(KVServiceID serviceId, LinkedBlockingQueue<IKVCall> queue, ARecordType schema) throws IllegalStateException{
		if(getQueryQueue(serviceId) != null){
			//throw new IllegalStateException("Service Queue already exists for "+serviceId.toString());		//TODO Revise Exception Type
			System.out.println("<!><!><!><!><!> Service Queue already exists for "+serviceId.toString());
			return;
		}
		System.out.println("Query Queue Registered for service "+serviceId);
		serviceMap.put(serviceId, queue);
		schemaMap.put(serviceId, schema);
	}
	
	public void registerOutputQueue(int queryId, LinkedBlockingQueue<Object[]> queue){
		if(getOutputQueue(queryId) != null){
			throw new IllegalStateException("Output Queue already exists for "+queryId);		//TODO Revise Exception Type
		}
		outputmap.put(queryId, queue);
	}
	
	public void removeOutputQueue(int queryId){
		outputmap.remove(queryId);
		System.out.println(">>>> Removing Output Queue for query "+queryId);
	}
	
	public List<KVServiceID> listAllRegisteredServices(){
		List<KVServiceID> l = new LinkedList<KVServiceID>();
		for(KVServiceID s : serviceMap.keySet()){
			l.add( new KVServiceID(s.getDataverseName(), s.getDatasetName()) );
		}
		return l;
	}
	
	public List<KVServiceID> listRegisteredServicesInDV(String dvName){
		List<KVServiceID> l = new LinkedList<KVServiceID>();
		for(KVServiceID s : serviceMap.keySet()){
			if(s.getDataverseName().equals(dvName)){
				l.add( new KVServiceID(s.getDataverseName(), s.getDatasetName()) );
			}	
		}
		return l;
	}
	
	
	public ARecordType getServiceSchema(KVServiceID serviceId){
		return schemaMap.get(serviceId);
	}
}

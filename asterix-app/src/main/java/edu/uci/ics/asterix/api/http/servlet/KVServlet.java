package edu.uci.ics.asterix.api.http.servlet;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.concurrent.LinkedBlockingQueue;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.kvs.GetCall;
import edu.uci.ics.asterix.kvs.IKVCall;
import edu.uci.ics.asterix.kvs.KVCallParser;
import edu.uci.ics.asterix.kvs.KVSResponseDispatcherOperatorDescriptor;
import edu.uci.ics.asterix.kvs.KVServiceProvider;
import edu.uci.ics.asterix.kvs.KVRequestDispatcherOperatorDescriptor;
import edu.uci.ics.asterix.kvs.KVRequestHandlerOperatorDescriptor;
import edu.uci.ics.asterix.kvs.KVServiceID;
import edu.uci.ics.asterix.kvs.KVUtils;
import edu.uci.ics.asterix.kvs.MToNPartitioningTimeTriggeredConnectorDescriptor;
import edu.uci.ics.asterix.kvs.PutCall;
import edu.uci.ics.asterix.metadata.MetadataException;
import edu.uci.ics.asterix.metadata.MetadataManager;
import edu.uci.ics.asterix.metadata.MetadataTransactionContext;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledDatasetDecl;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledMetadataDeclarations;
import edu.uci.ics.asterix.metadata.declared.AqlMetadataProvider;
import edu.uci.ics.asterix.om.base.AInt32;
import edu.uci.ics.asterix.om.base.ARecord;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.transaction.management.exception.ACIDException;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionManagementConstants.LockManagerConstants.LockMode;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.algebricks.common.utils.Triple;
import edu.uci.ics.hyracks.api.client.HyracksConnection;
import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;
import edu.uci.ics.hyracks.api.constraints.PartitionConstraintHelper;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.data.std.accessors.PointableBinaryComparatorFactory;
import edu.uci.ics.hyracks.data.std.accessors.PointableBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFactory;
import edu.uci.ics.hyracks.dataflow.std.file.ConstantFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.misc.PrinterOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.util.StringSerializationUtils;
import edu.uci.ics.hyracks.storage.am.btree.dataflow.BTreeDataflowHelperFactory;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndex;
import edu.uci.ics.hyracks.storage.am.common.dataflow.IIndexRegistryProvider;
import edu.uci.ics.hyracks.storage.common.IStorageManagerInterface;

public class KVServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;
	
	private static long queryArrivalTime = -1;			//TODO used for benchmarking - Should be removed eventually
	private static long prevQueryDepartureTime = -1;	//TODO used for benchmarking - Should be removed eventually
	private PrintWriter queryPw;
	private String qDumpPathPrefix;
	int qDumpId;
	int qCounter = 0;
	private PrintWriter waitPw;
	private String wDumpPathPrefix;
	int wDumpId;
	int wCounter = 0;
	private final int pace = 500;
	
	
	KVServiceID sId;
    KVCallProcessor p;
    
    
    public KVServlet() throws FileNotFoundException{
    	sId = new KVServiceID();
    	p = new KVCallProcessor();
    	
    	qDumpPathPrefix = "/data/pouria/dump/q/qd-"; //"/home/pouria/dump/q/qd-";
    	qDumpId = 0;
    	queryPw = new PrintWriter(qDumpPathPrefix+(qDumpId++) );
    	wDumpPathPrefix = "/data/pouria/dump/w/wd-"; //"/home/pouria/dump/w/wd-";
    	wDumpId = 0;
    	waitPw = new PrintWriter(wDumpPathPrefix+(wDumpId++));
    	
    }
    
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        
    	//-------
    	long t = System.currentTimeMillis();
    	if(queryArrivalTime != -1){
    		throw new ServletException("Rewritting query arival time");
    	}
    
    	if(prevQueryDepartureTime != -1){
    		waitPw.println( (t-prevQueryDepartureTime) );
    		wCounter++;
    		if(wCounter % pace == 0){
    			waitPw.close();
    			waitPw = new PrintWriter(wDumpPathPrefix+(wDumpId++));
    		}
    	}
    	prevQueryDepartureTime = -1;
    	queryArrivalTime = t;
    	//-------
    	
    	
    	PrintWriter out = resp.getWriter();
    	
    	String uri = req.getRequestURI();
    	List<String> parsedUri = parseURI(uri);
    	String getQuery = req.getQueryString();
    	
    	int uriSize = parsedUri.size();
    	if(getQuery != null && parsedUri.size() != 2){
    		out.println("\nInvalid get() call, missing or invalid dataverse and/or dataset name\n");
    		return;
    	}
    	
    	switch(uriSize){
    	case 0:		//List all services
    		List<KVServiceID> allList = KVServiceProvider.INSTANCE.listAllRegisteredServices();
    		if(allList.size() == 0){
    			out.println("\nNo dataset is registred for key service\n");
    			return;
    		}
    		out.println("\nAll Registered key service(s):\n");
    		for(KVServiceID s : allList){
    			out.println(s.toString());
    		}
    		out.println("\n");
    		return;
    	case 1:		//List services in the DV
    		List<KVServiceID> allInDv = KVServiceProvider.INSTANCE.listRegisteredServicesInDV(parsedUri.get(0));
    		if(allInDv.size() == 0){
    			out.println("\nNo dataset is registred for key service in dataverse "+parsedUri.get(0)+"\n");
    			return;
    		}
    		out.println("\nAll Registered datasets for key service(s) in dataverse "+parsedUri.get(0)+":\n");
    		for(KVServiceID s : allInDv){
    			out.println(s.getDatasetName().toString());
    		}
    		out.println("\n");
    		return;
    	case 2:		//Regular GET or Schema for DS
    		sId.reset(parsedUri.get(0), parsedUri.get(1));
			ARecordType schema = KVServiceProvider.INSTANCE.getServiceSchema( sId );
			if(schema == null){
				out.println("\nDataset "+sId.getDatasetName()+" in dataverse "+sId.getDataverseName()+" does not exist or is not registered for key service\n");
				return;
			}
			
    		if(getQuery == null){
    			String isOpen = schema.isOpen() ? "Open" : "Closed";
    			out.println("\n"+isOpen);
    			String[] fieldNames = schema.getFieldNames();
    			IAType[] fieldTypes = schema.getFieldTypes();
    			for(int i=0; i<fieldNames.length; i++){
    				out.println(fieldNames[i]+" : "+fieldTypes[i]);
    			}
    			out.println("\n");
    			return;
    		}
    		break;
    	default:	//Invalid URI
    		out.println("\nInvalid URI for get call in key service\n");
    		return;
    	}
    	
    	String dvName = parsedUri.get(0);
    	String dsName = parsedUri.get(1);
    	
    	
    	StringTokenizer qSt = new StringTokenizer(getQuery, "=&");
    	List< Pair<String, String> > keyFields = new LinkedList< Pair<String, String> >();
    	while(qSt.hasMoreTokens()){
    		String nextColName = qSt.nextToken();
    		String nextColVal = (qSt.nextToken()).replaceAll("%20", " ");
    		keyFields.add( new Pair<String, String>(nextColName, nextColVal) );
    	}

    	String r = "";
    	int queryId = KVServiceProvider.INSTANCE.generateNextQueryId();

    	try {
    		LinkedBlockingQueue<Object[]> outputQueue = new LinkedBlockingQueue<Object[]>();
    		KVServiceProvider.INSTANCE.registerOutputQueue(queryId, outputQueue);
    		KVServiceProvider.INSTANCE.storeStartTime(queryId, queryArrivalTime);
    		p.processGet(keyFields,/* hcc,*/ queryId, dvName, dsName);
    		Object[] result = outputQueue.take();
    		long e = System.currentTimeMillis();
    		long b = KVServiceProvider.INSTANCE.getStartTime(queryId);
    		r = p.interpretResult(result);
    		
    		queryPw.println( (e - b) );
    		qCounter++;
    		if(qCounter % pace == 0){
    			queryPw.close();
    			queryPw = new PrintWriter(qDumpPathPrefix+(qDumpId++) );
    		}
    		
    		

    	} catch (ACIDException e) {
    		r = "\nException in doGet() in KVS Servlet\n";
    		e.printStackTrace();
    	} catch (MetadataException e) {
    		e.printStackTrace();
    	} catch (AlgebricksException e) {
    		e.printStackTrace();
    	} catch (AsterixException e) {
    		e.printStackTrace();
    	} catch (Exception e) {
    		e.printStackTrace();
    	}

    	KVServiceProvider.INSTANCE.removeOutputQueue(queryId);
    	out.println(r);
    	
    	
    	queryArrivalTime = -1;
    	prevQueryDepartureTime = System.currentTimeMillis();
    }

    protected void doPut(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    	PrintWriter out = resp.getWriter();
    	
    	String uri = req.getRequestURI();
    	List<String> parsedUri = parseURI(uri);
    	
    	if(parsedUri.size() != 2){
    		out.println("\nInvalid put() call, missing or invalid dataverse and/or dataset name\n");
    		return;
    	}
    	
    	String dvName = parsedUri.get(0);
    	String dsName = parsedUri.get(1);
    	
    	sId.reset(dvName, dsName);
		ARecordType schema = KVServiceProvider.INSTANCE.getServiceSchema( sId );
		if(schema == null){
			out.println("\nDataset "+sId.getDatasetName()+" in dataverse "+sId.getDataverseName()+" does not exist or is not registered for key service\n");
			return;
		}
    	
    	String r = "";
        int queryId = KVServiceProvider.INSTANCE.generateNextQueryId();
    	
        LinkedBlockingQueue<Object[]> outputQueue = new LinkedBlockingQueue<Object[]>();
   	 	KVServiceProvider.INSTANCE.registerOutputQueue(queryId, outputQueue);
        
        Object[] result;

        Map<String, String[]> m = req.getParameterMap();
        int ps = m.keySet().size();
        if(ps != 1){
        	r = "Invalid number of parameters "+ps+" for put";
        }
        else{
        	try {
        		String value = m.keySet().iterator().next();
        		System.out.println(">>>>>>>> Value is "+value);
        		p.processPut(value, queryId, dvName, dsName);
        		result = outputQueue.take();
        		r = p.interpretResult(result);
        		System.out.println("Got the result back for query "+queryId+" in doPut()");
        	} catch (Exception e) {
        		e.printStackTrace();
        	}
        }
        KVServiceProvider.INSTANCE.removeOutputQueue(queryId);
        out.println(r);
    }
    
    
    private List<String> parseURI(String uri){
    	StringTokenizer uriSt = new StringTokenizer(uri, "/");
    	uriSt.nextToken();	//kvs (Ignoring service identifier)
    	LinkedList<String> l = new LinkedList<String>();
    	while(uriSt.hasMoreTokens()){
    		l.add( uriSt.nextToken() );
    	}
    	return l;
    }
}

class KVCallProcessor{
	KVUtils.KVResponseType[] tags = KVUtils.KVResponseType.values();
	KVServiceID kvsId;
	
	public KVCallProcessor(){
		kvsId = new KVServiceID();
		
	}
	
	
	public void processGet(List<Pair<String, String>> keysValues, int queryId, String dataverseName, String datasetName) throws Exception{
		kvsId.reset(dataverseName, datasetName);
		LinkedBlockingQueue<IKVCall> queue = KVServiceProvider.INSTANCE.getQueryQueue(kvsId);
		queue.put( new GetCall(queryId, keysValues) );
	}
	
	public void processPut(String admValueString, int queryId, String dataverseName, String datasetName) throws Exception{
		kvsId.reset(dataverseName, datasetName);
		LinkedBlockingQueue<IKVCall> queue = KVServiceProvider.INSTANCE.getQueryQueue(kvsId);
		queue.put( new PutCall(queryId, admValueString) );
	}
	
	public String interpretResult(Object[] result){
		StringBuffer r = new StringBuffer("\n");
		AInt32 ix = (AInt32) (result[2]);
		KVUtils.KVResponseType tag = tags[ ix.getIntegerValue().intValue() ];	//TODO Revise direct index access
		switch(tag){
		case REGULAR:
    		r.append( StringSerializationUtils.toString(result[result.length - 1]) );
    		return r.append("\n").toString();
		case EMPTY:
			return r.append("Empty Result\n").toString();
		case MESSAGE:
			return r.append( StringSerializationUtils.toString(result[3])+"\n" ).toString();
		default:
			return r.append( "Unknown Response Type\n" ).toString();
		}
		
	}
}

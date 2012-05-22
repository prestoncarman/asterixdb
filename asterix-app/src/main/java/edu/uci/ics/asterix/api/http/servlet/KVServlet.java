package edu.uci.ics.asterix.api.http.servlet;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.concurrent.LinkedBlockingQueue;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import edu.uci.ics.asterix.common.config.GlobalConfig;
import edu.uci.ics.asterix.common.context.AsterixIndexRegistryProvider;
import edu.uci.ics.asterix.common.context.AsterixStorageManagerInterface;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.ARecordSerializerDeserializer;
import edu.uci.ics.asterix.formats.nontagged.AqlBinaryHashFunctionFactoryProvider;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.formats.nontagged.AqlTypeTraitProvider;
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
	private static final String HYRACKS_CONNECTION_ATTR = "edu.uci.ics.asterix.HYRACKS_CONNECTION";
	private static final long serialVersionUID = 1L;
	
    KVCallProcessor p;
    
    
    public KVServlet(){
    	p = new KVCallProcessor();
    }
    
    /*
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    	//TODO This method should eventually be removed (Registration will happen through web-Interface during DS-Creation)
    	String uri = req.getRequestURI();
    	StringTokenizer uriSt = new StringTokenizer(uri, "/");
    	uriSt.nextToken();	//kvs (Ignoring service identifier)
    	String dvName = uriSt.nextToken();
    	String dsName = uriSt.nextToken();
    	IHyracksClientConnection hcc = getHcc();
    	String r = "";
    	try {
			r = p.processReg(hcc, dvName, dsName);
		} catch (Exception e) {
			r = "Unsuccessfull Service Registration for Dataset: "+dsName+" in Dataverse "+dvName;
			e.printStackTrace();
		}
		PrintWriter out = resp.getWriter();
		out.println(r);
    }
    */
    
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        
    	PrintWriter out = resp.getWriter();
    	
    	String uri = req.getRequestURI();
    	List<String> parsedUri = parseURI(uri);
    	String getQuery = req.getQueryString();
    	
    	int uriSize = parsedUri.size();
    	if(getQuery != null && parsedUri.size() != 2){
    		out.println("\nInvalid get() call, Missing dataverse and/or dataset name\n");
    		return;
    	}
    	
    	switch(uriSize){
    	case 0:		//List all services
    		List<KVServiceID> allList = KVServiceProvider.INSTANCE.listAllRegisteredServices();
    		if(allList.size() == 0){
    			out.println("\nNo dataset registred for key service\n");
    			return;
    		}
    		for(KVServiceID s : allList){
    			out.println(s.toString()+"\n");
    		}
    		return;
    	case 1:		//List services in the DV
    		List<KVServiceID> allInDv = KVServiceProvider.INSTANCE.listRegisteredServicesInDV(parsedUri.get(0));
    		if(allInDv.size() == 0){
    			out.println("\nNo dataset registred for key service in dataverse "+parsedUri.get(0)+"\n");
    			return;
    		}
    		for(KVServiceID s : allInDv){
    			out.println(s.toString()+"\n");
    		}
    		return;
    	case 2:		//Regular GET or Schema for DS
    		if(getQuery == null){
    			KVServiceID sId = new KVServiceID(parsedUri.get(0), parsedUri.get(1));
    			ARecordType schema = KVServiceProvider.INSTANCE.getServiceSchema( sId );
    			if(schema == null){
    				out.println("\nDataset "+sId.getDatasetName()+" in dataverse "+sId.getDataverseName()+" does not exist or does not registered for key service\n");
    				return;
    			}
    			String isOpen = schema.isOpen() ? "Open" : "Closed";
    			out.println("\n"+isOpen);
    			String[] fieldNames = schema.getFieldNames();
    			IAType[] fieldTypes = schema.getFieldTypes();
    			for(int i=0; i<fieldNames.length; i++){
    				out.println(fieldNames[i]+" :\t"+fieldTypes[i]);
    			}
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
    	//IHyracksClientConnection hcc = getHcc();
    	int queryId = KVServiceProvider.INSTANCE.generateNextQueryId();

    	try {
    		LinkedBlockingQueue<Object[]> outputQueue = new LinkedBlockingQueue<Object[]>();
    		KVServiceProvider.INSTANCE.registerOutputQueue(queryId, outputQueue);
    		p.processGet(keyFields,/* hcc,*/ queryId, dvName, dsName);
    		Object[] result = outputQueue.take();
    		r = "";
    		for (int i = 2; i < result.length; ++i) {
    			r += StringSerializationUtils.toString(result[i]);
    			r += ", ";
    		}
    		r += "\n";

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
    }

    protected void doPut(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    	PrintWriter out = resp.getWriter();
    
    	String uri = req.getRequestURI();
    	StringTokenizer uriSt = new StringTokenizer(uri, "/");
    	uriSt.nextToken();	//kvs (Ignoring service identifier)
    	String dvName = uriSt.nextToken();
    	String dsName = uriSt.nextToken();
    	
    	String r = "";
    	//IHyracksClientConnection hcc = getHcc();
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
        		p.processPut(value, /*hcc,*/ queryId, dvName, dsName);
        		result = outputQueue.take();
        		for (int i = 2; i < result.length; ++i) {
        			r += StringSerializationUtils.toString(result[i]);
        			r += ", ";
        		}
        		r += "\n";
        		System.out.println("Got the result back for query "+queryId+" in doPut()");
        	} catch (Exception e) {
        		e.printStackTrace();
        	}
        }
        KVServiceProvider.INSTANCE.removeOutputQueue(queryId);
        out.println(r);
    }
    
    /*
    private synchronized IHyracksClientConnection getHcc(){
    	//TODO Look at APIServlet and generalize this part (using finals)
        ServletContext context = getServletContext();
        IHyracksClientConnection hcc;
        synchronized (context) {
            hcc = (IHyracksClientConnection) context.getAttribute(HYRACKS_CONNECTION_ATTR);
            if (hcc == null) {
                try {
					hcc = new HyracksConnection("128.195.52.106", 1098);
				} catch (Exception e) {
					e.printStackTrace();
				}
                context.setAttribute(HYRACKS_CONNECTION_ATTR, hcc);
            }
        }
        return hcc;
    }
    */
    
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
	/*
	public String processReg(IHyracksClientConnection hcc, String dataverseName, String datasetName) throws Exception{
		MetadataTransactionContext mdTxnCtx = MetadataManager.INSTANCE.beginTransaction();
		MetadataManager.INSTANCE.lock(mdTxnCtx, LockMode.SHARED);
		AqlCompiledMetadataDeclarations acmd = KVUtils.generateACMD(mdTxnCtx, dataverseName);
		
		acmd.connectToDataverse(dataverseName);
		AqlCompiledDatasetDecl acdd = KVUtils.generateACDD(acmd, datasetName);
		
		IAType[] keyTypes = KVUtils.getKeyTypes(acdd);
		List<String> partitionKeys = KVUtils.getPartitioningKeys(acdd);
		String ixName = KVUtils.getPixName(acdd);
		Pair<IFileSplitProvider, AlgebricksPartitionConstraint> fsap = KVUtils.getFileSpltAndConstraint(acmd, datasetName, ixName);
		ConstantFileSplitProvider fs = (ConstantFileSplitProvider) fsap.first;
		AlgebricksAbsolutePartitionConstraint partConst = (AlgebricksAbsolutePartitionConstraint) fsap.second;
		
		
		ARecordType record = KVUtils.getItemType(acmd, acdd);
		
		Triple<ITypeTraits[], IBinaryComparatorFactory[], ISerializerDeserializer[]> triple = KVUtils.getTreeOutputRec(acmd, acdd);
		ITypeTraits[] tt = triple.first;
		IBinaryComparatorFactory[] bcf = triple.second;
		ISerializerDeserializer[] isd = triple.third;
		
		acmd.disconnectFromDataverse();
		MetadataManager.INSTANCE.commitTransaction(mdTxnCtx);
		
		JobSpecification spec = generateServiceJobSpec(dataverseName, datasetName, keyTypes, tt, bcf, fs, isd, record, partitionKeys, partConst);
		
		JobId jobId = hcc.createJob(GlobalConfig.HYRACKS_APP_NAME, spec);
		System.out.println("Going to Start a Job in REG");
		hcc.start(jobId);
		
		return "REG call executed Successfully";
	}
	*/
	
	private JobSpecification generateServiceJobSpec(String dvName, String dsName, IAType[] keyType, ITypeTraits[] typeTraits, IBinaryComparatorFactory[] comparatorFactories, IFileSplitProvider fileSplitProvider, ISerializerDeserializer[] res, ARecordType record, List<String> partitioningKeys, AlgebricksAbsolutePartitionConstraint parts) throws Exception{
		
		JobSpecification spec = new JobSpecification();
		KVRequestDispatcherOperatorDescriptor reqDisp = 
			new KVRequestDispatcherOperatorDescriptor(spec, keyType, dvName, dsName, record, partitioningKeys, 3000);	//TODO Change it to use all keys (We need dynamic tb for manager based on num of fields)
		
		IStorageManagerInterface storageManager = AsterixStorageManagerInterface.INSTANCE;
		IIndexRegistryProvider<IIndex> indexRegistryProvider = AsterixIndexRegistryProvider.INSTANCE;
		
		//ITreeIndexFrameFactory interiorFrameFactory = KVUtils.getInteriorFrameFactory(typeTraits); 
	   // ITreeIndexFrameFactory leafFrameFactory = KVUtils.getLeafFrameFactory(typeTraits); 
	    
	    ISerializerDeserializer[] kvRespSerDe = new ISerializerDeserializer[res.length + 2];
	    kvRespSerDe[0] = AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT32);
	    kvRespSerDe[1] = AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT32);
	    for(int i=2; i<kvRespSerDe.length; i++){
	    	kvRespSerDe[i] = res[i-2];
	    }
	    RecordDescriptor kvRespRecDesc = new RecordDescriptor(kvRespSerDe);
	    
		
		KVRequestHandlerOperatorDescriptor reqHandler = 
				new KVRequestHandlerOperatorDescriptor(spec, kvRespRecDesc, 
						storageManager, indexRegistryProvider, fileSplitProvider, 
							/*interiorFrameFactory, leafFrameFactory,*/ typeTraits, 
								comparatorFactories, new BTreeDataflowHelperFactory(), keyType.length, 3000);
		
		
		//PrinterOperatorDescriptor printer = new PrinterOperatorDescriptor(spec);
		KVSResponseDispatcherOperatorDescriptor respDisp = new KVSResponseDispatcherOperatorDescriptor(spec);
		
		String[] locs = parts.getLocations();
		System.out.println("\n\n\nLocations List:");
		for(String l : locs){
			System.out.println("\t"+l);
		}
		System.out.println("\n\n\n");
		
		PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, reqDisp, parts.getLocations() /*"nc1", "nc2"*/);
		PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, reqHandler, parts.getLocations() /*"nc1", "nc2"*/);
		//PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, "nc1", "nc2");
		PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, respDisp, parts.getLocations() /*"nc1", "nc2"*/);
		
		
		IBinaryHashFunctionFactory[] hashFactories1 = new IBinaryHashFunctionFactory[keyType.length];
		int[] keysIx = new int[keyType.length];
		for(int i=0; i<keysIx.length; i++){
			keysIx[i] = (3+i);
			hashFactories1[i] = AqlBinaryHashFunctionFactoryProvider.INSTANCE.getBinaryHashFunctionFactory(keyType[i] /*BuiltinType.ASTRING*/);	//TODO Generalize it to support other types
		}
		ITuplePartitionComputerFactory tpcf1 = new FieldHashPartitionComputerFactory(keysIx, hashFactories1);
		IConnectorDescriptor con1 = new MToNPartitioningTimeTriggeredConnectorDescriptor(spec, tpcf1, 7000);
		
		IBinaryHashFunctionFactory[] hashFactories2 = new IBinaryHashFunctionFactory[]{AqlBinaryHashFunctionFactoryProvider.INSTANCE.getBinaryHashFunctionFactory(BuiltinType.AINT32)};	
		ITuplePartitionComputerFactory tpcf2 = new FieldHashPartitionComputerFactory(new int[]{0}, hashFactories2);
		IConnectorDescriptor con2 = new MToNPartitioningTimeTriggeredConnectorDescriptor(spec, tpcf2, 3000);
		
		spec.connect(con1, reqDisp, 0, reqHandler, 0);
	    //spec.connect(con2, reqHandler, 0, printer, 0);
	    spec.connect(con2, reqHandler, 0, respDisp, 0);
		//spec.addRoot(printer);
	    spec.addRoot(respDisp); 
	    return spec;
	}
}

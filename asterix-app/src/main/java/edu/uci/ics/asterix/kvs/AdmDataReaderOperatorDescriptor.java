package edu.uci.ics.asterix.kvs;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;

public class AdmDataReaderOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {
	
	private static final long serialVersionUID = 1L;
	
	private final ARecordType recType;
	private final String inputFile;
	
	public AdmDataReaderOperatorDescriptor(JobSpecification spec, String inputFile, RecordDescriptor recDesc, ARecordType recType) {
		super(spec, 0, 1);
		this.inputFile = inputFile;
		this.recType = recType;
		recordDescriptors[0] = recDesc;
	}
	

	@Override
	public IOperatorNodePushable createPushRuntime(IHyracksTaskContext ctx,
			IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {
		
		return new AdmDataReaderOperatorNodePushable(ctx, recType, new AdmFixedStream(inputFile));
		
		//return new AdmDataReaderOperatorNodePushable(ctx, recType, new AdmDataStream(ctx.getFrameSize(), inputFile));
		
		/*
		try {
			return new AdmDataReaderOperatorNodePushable(ctx, recType, new FileInputStream(inputFile));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		return null;
		*/
	}

}

class AdmFixedStream extends InputStream {
	private byte[] data;
	int index;
		
		//Assumption: inputFile contains one line that is the adm record we want to serialize
	public AdmFixedStream(String inputFile){
		try {
			data = readFile(inputFile);
		} catch (Exception e) {
			e.printStackTrace();
		}
		index = 0;
	}
	
	@Override
	public int read() throws IOException {
		if(index >= data.length){
			return -1;
		}
		return data[index++];
	}
	
	private byte[] readFile(String inputFile) throws Exception{
		BufferedReader in = new BufferedReader(new FileReader(inputFile));
		String str;
		if ((str = in.readLine()) != null) {
			return (str.trim()).getBytes();
		}
		return null;
	}
	
}

class AdmDataStream extends InputStream {
	
	private ArrayList<String> records;
	private ByteBuffer buffer;
	private int capacity;
	
	public AdmDataStream(int capacity, String inputFile) {
		try {
			this.records = readFile(inputFile);
			this.capacity = capacity;
			buffer = ByteBuffer.allocate(capacity);
			init();
		} catch (Exception e) {
			System.err.println("!!!!!!! Exception in AdmDataStream constructor");
			e.printStackTrace();
		}
	}
	
	private void init(){
		buffer.position(0);
        buffer.limit(capacity);
        for (String r : records) {
            buffer.put(r.getBytes());
           // buffer.put("\n".getBytes());
        }
        buffer.flip();
	}

	@Override
	public int read() throws IOException {
		if(hasRemaining()){
			return buffer.get();
		}
		return -1;
	}
	
	public boolean hasRemaining(){
		return buffer.hasRemaining();
	}
	
	private ArrayList<String> readFile(String inputFile) throws Exception {
		ArrayList<String> records = new ArrayList<String>();
		BufferedReader in = new BufferedReader(new FileReader(inputFile));
		String str;
		while ((str = in.readLine()) != null) {
			records.add( new String(str.trim()) );
		}
		return records;
	}
	
}

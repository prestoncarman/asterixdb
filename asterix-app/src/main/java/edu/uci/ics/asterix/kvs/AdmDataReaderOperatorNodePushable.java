package edu.uci.ics.asterix.kvs;

import java.io.InputStream;

import edu.uci.ics.asterix.external.data.parser.ADMStreamParser;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.runtime.operators.file.AdmSchemafullRecordParserFactory;
import edu.uci.ics.asterix.runtime.operators.file.AdmTupleParser;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParser;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParserFactory;

public class AdmDataReaderOperatorNodePushable extends AbstractUnaryOutputSourceOperatorNodePushable{
	
	//private AdmTupleParser parser;
	//private ADMStreamParser parser;
	private ITupleParser parser;
	private InputStream inputStream;
	
	public AdmDataReaderOperatorNodePushable(IHyracksTaskContext ctx, ARecordType recType, InputStream inputStream){
		//parser = new AdmTupleParser(ctx, recType);
		//parser =  new ADMStreamParser();
		//parser.setInputStream(inputStream);
		//parser.initialize(recType, ctx);
		this.parser = ( new AdmSchemafullRecordParserFactory(recType) ).createTupleParser(ctx);
		this.inputStream = inputStream;
	}
	
	@Override
    public void initialize() throws HyracksDataException {
		writer.open();
		parser.parse(inputStream, writer);
		//parser.parse(writer);
		System.out.println("Done with parsing");
		writer.close();
    }

}

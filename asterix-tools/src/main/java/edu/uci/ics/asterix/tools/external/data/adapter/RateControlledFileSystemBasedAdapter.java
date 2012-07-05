/*
 * Copyright 2009-2012 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.tools.external.data.adapter;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.external.adapter.factory.IGenericDatasourceAdapterFactory;
import edu.uci.ics.asterix.external.dataset.adapter.FileSystemBasedAdapter;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.runtime.operators.file.ADMDataParser;
import edu.uci.ics.asterix.runtime.operators.file.AbstractTupleParser;
import edu.uci.ics.asterix.runtime.operators.file.DelimitedDataParser;
import edu.uci.ics.asterix.runtime.operators.file.IDataParser;
import edu.uci.ics.hyracks.algebricks.common.exceptions.NotImplementedException;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IValueParserFactory;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParser;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParserFactory;

public class RateControlledFileSystemBasedAdapter extends
		FileSystemBasedAdapter {

	public static final String KEY_FILE_SYSTEM = "fs";
	public static final String KEY_FORMAT = "format";

	public static final String LOCAL_FS = "localfs";
	public static final String HDFS = "hdfs";

	public static final String FORMAT_DELIMITED_TEXT = "delimited-text";
	public static final String FORMAT_ADM = "adm";

	private final FileSystemBasedAdapter coreAdapter;
	private final Map<String, String> configuration;
	private final String fileSystem;
	private final String format;

	public RateControlledFileSystemBasedAdapter(ARecordType atype,
			Map<String, String> configuration) throws Exception {
		super(atype);
		String adapterFactoryClass = null;
		fileSystem = configuration.get(KEY_FILE_SYSTEM);
		if (fileSystem.equalsIgnoreCase(LOCAL_FS)) {
			adapterFactoryClass = "edu.uci.ics.asterix.external.adapter.factory.NCFileSystemAdapterFactory";
		} else if (fileSystem.equals(HDFS)) {
			adapterFactoryClass = "edu.uci.ics.asterix.external.adapter.factory.HDFSAdapterFactory";
		}

		format = configuration.get(KEY_FORMAT);
		IGenericDatasourceAdapterFactory adapterFactory = (IGenericDatasourceAdapterFactory) Class
				.forName(adapterFactoryClass).newInstance();
		coreAdapter = (FileSystemBasedAdapter) adapterFactory.createAdapter(
				configuration, atype);
		this.configuration = configuration;
	}

	protected ITupleParser getTupleParser() throws Exception {
		ITupleParser parser = null;
		if (format.equals(FORMAT_DELIMITED_TEXT)) {
			parser = getRateControlledDelimitedDataTupleParser((ARecordType) atype);
		} else if (format.equals(FORMAT_ADM)) {
			parser = getRateControlledADMDataTupleParser((ARecordType) atype);
		} else {
			throw new IllegalArgumentException(" format "
					+ configuration.get(KEY_FORMAT) + " not supported");
		}
		return parser;

	}

	@Override
	public InputStream getInputStream(int partition) throws IOException {
		return coreAdapter.getInputStream(partition);
	}

	@Override
	public void initialize(IHyracksTaskContext ctx) throws Exception {
		coreAdapter.initialize(ctx);
		this.ctx = ctx;
	}

	@Override
	public void configure(Map<String, String> arguments) throws Exception {
		coreAdapter.configure(arguments);
	}

	@Override
	public AdapterDataFlowType getAdapterDataFlowType() {
		return coreAdapter.getAdapterDataFlowType();
	}

	@Override
	public AdapterType getAdapterType() {
		return coreAdapter.getAdapterType();
	}

	protected ITupleParser getRateControlledDelimitedDataTupleParser(
			ARecordType recordType) throws AsterixException {
		ITupleParser parser;
		int n = recordType.getFieldTypes().length;
		IValueParserFactory[] fieldParserFactories = new IValueParserFactory[n];
		for (int i = 0; i < n; i++) {
			ATypeTag tag = recordType.getFieldTypes()[i].getTypeTag();
			IValueParserFactory vpf = typeToValueParserFactMap.get(tag);
			if (vpf == null) {
				throw new NotImplementedException(
						"No value parser factory for delimited fields of type "
								+ tag);
			}
			fieldParserFactories[i] = vpf;
		}
		String delimiterValue = (String) configuration.get(KEY_DELIMITER);
		if (delimiterValue != null && delimiterValue.length() > 1) {
			throw new AsterixException("improper delimiter");
		}

		Character delimiter = delimiterValue.charAt(0);
		parser = new RateControlledTupleParserFactory(recordType,
				fieldParserFactories, delimiter, configuration)
				.createTupleParser(ctx);
		return parser;
	}

	protected ITupleParser getRateControlledADMDataTupleParser(
			ARecordType recordType) throws AsterixException {
		ITupleParser parser = null;
		try {
			parser = new RateControlledTupleParserFactory(recordType,
					configuration).createTupleParser(ctx);

			return parser;
		} catch (Exception e) {
			throw new AsterixException(e);
		}

	}

}

class RateControlledTupleParserFactory implements ITupleParserFactory {

	private final ARecordType recordType;
	private final IDataParser dataParser;
	private final Map<String, String> configuration;

	public RateControlledTupleParserFactory(ARecordType recordType,
			IValueParserFactory[] valueParserFactories, char fieldDelimiter,
			Map<String, String> configuration) {
		this.recordType = recordType;
		dataParser = new DelimitedDataParser(recordType, valueParserFactories,
				fieldDelimiter);
		this.configuration = configuration;
	}

	public RateControlledTupleParserFactory(ARecordType recordType,
			Map<String, String> configuration) {
		this.recordType = recordType;
		dataParser = new ADMDataParser();
		this.configuration = configuration;
	}

	@Override
	public ITupleParser createTupleParser(IHyracksTaskContext ctx) {
		return new RateControlledTupleParser(ctx, recordType, dataParser,
				configuration);
	}

}

class RateControlledTupleParser extends AbstractTupleParser {

	private final IDataParser dataParser;
	private final Map<String, String> configuration;
	private final long interTupleInterval;
	private final boolean delayConfigured;

	public static final String INTER_TUPLE_INTERVAL = "tuple-interval";

	public RateControlledTupleParser(IHyracksTaskContext ctx,
			ARecordType recType, IDataParser dataParser,
			Map<String, String> configuration) {
		super(ctx, recType);
		this.dataParser = dataParser;
		this.configuration = configuration;
		String propValue = configuration.get(INTER_TUPLE_INTERVAL);
		if (propValue != null) {
			interTupleInterval = Long.parseLong(propValue);
		} else {
			interTupleInterval = 0;
		}
		delayConfigured = interTupleInterval != 0;
	}

	@Override
	public IDataParser getDataParser() {
		return dataParser;
	}

	@Override
	public void parse(InputStream in, IFrameWriter writer)
			throws HyracksDataException {

		appender.reset(frame, true);
		IDataParser parser = getDataParser();
		try {
			parser.initialize(in, recType, true);
			while (true) {
				tb.reset();
				if (!parser.parse(tb.getDataOutput())) {
					break;
				}
				tb.addFieldEndOffset();
				if (delayConfigured) {
					try {
						introduceDelay();
					} catch (InterruptedException ie) {
						throw new HyracksDataException(ie);
					}
				}
				addTupleToFrame(writer);
			}
			if (appender.getTupleCount() > 0) {
				FrameUtils.flushFrame(frame, writer);
			}
		} catch (AsterixException ae) {
			throw new HyracksDataException(ae);
		} catch (IOException ioe) {
			throw new HyracksDataException(ioe);
		}
	}

	private void introduceDelay() throws InterruptedException {
		Thread.currentThread().sleep(interTupleInterval);
	}

}

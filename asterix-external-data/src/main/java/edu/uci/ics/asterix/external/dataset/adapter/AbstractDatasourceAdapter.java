/*
 * Copyright 2009-2011 by The Regents of the University of California
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
package edu.uci.ics.asterix.external.dataset.adapter;

import java.util.HashMap;
import java.util.Map;

import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IValueParserFactory;

/**
 * Represents the base class that is required to be extended by every
 * implementation of the IDatasourceAdapter interface.
 */
public abstract class AbstractDatasourceAdapter implements IDatasourceAdapter {

    private static final long serialVersionUID = -3510610289692452466L;

    protected Map<String, String> configuration;
    protected transient AlgebricksPartitionConstraint partitionConstraint;
    protected IAType atype;
    protected IHyracksTaskContext ctx;
    protected AdapterDataFlowType dataFlowType;
    protected AdapterType adapterType;
    protected boolean typeInfoRequired = false;

    protected static final HashMap<ATypeTag, IValueParserFactory> typeToValueParserFactMap = new HashMap<ATypeTag, IValueParserFactory>();
    protected static final HashMap<String, String> formatToParserFactoryMap = new HashMap<String, String>();

    public static final String KEY_FORMAT = "format";
    public static final String KEY_PARSER_FACTORY = "parser";

    public static final String FORMAT_DELIMITED_TEXT = "delimited-text";
    public static final String FORMAT_ADM = "adm";

    static {
        formatToParserFactoryMap.put(FORMAT_DELIMITED_TEXT,
                "edu.uci.ics.asterix.runtime.operators.file.AdmSchemafullRecordParserFactory");
        formatToParserFactoryMap.put(FORMAT_ADM,
                "edu.uci.ics.asterix.runtime.operators.file.NtDelimitedDataTupleParserFactory");
    }

    abstract public void initialize(IHyracksTaskContext ctx) throws Exception;

    abstract public void configure(Map<String, String> arguments) throws Exception;

    abstract public AdapterDataFlowType getAdapterDataFlowType();

    abstract public AdapterType getAdapterType();

    public AlgebricksPartitionConstraint getPartitionConstraint() {
        return partitionConstraint;
    }

    public String getAdapterProperty(String attribute) {
        return configuration.get(attribute);
    }

    public Map<String, String> getConfiguration() {
        return configuration;
    }

    public IAType getAdapterOutputType() {
        return atype;
    }

    public void setAdapterProperty(String property, String value) {
        configuration.put(property, value);
    }

    public boolean isTypeInfoRequired() {
        return typeInfoRequired;
    }

}

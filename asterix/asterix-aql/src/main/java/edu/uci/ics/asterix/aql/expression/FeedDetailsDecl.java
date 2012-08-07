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
package edu.uci.ics.asterix.aql.expression;

import java.util.List;
import java.util.Map;

import edu.uci.ics.asterix.om.functions.AsterixFunction;

public class FeedDetailsDecl extends InternalDetailsDecl {
    private final Map<String, String> properties;
    private final String adapterFactoryClassname;
    private final AsterixFunction function;

    public FeedDetailsDecl(String adapterFactoryClassName, Map<String, String> properties, AsterixFunction function,
            List<String> partitioningExpr, String nodegroupName) {
        super(nodegroupName, partitioningExpr);
        this.adapterFactoryClassname = adapterFactoryClassName;
        this.properties = properties;
        this.function = function;
    }

    public String getAdapterClassname() {
        return adapterFactoryClassname;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public AsterixFunction getFunction() {
        return function;
    }
}

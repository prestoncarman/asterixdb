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
package edu.uci.ics.asterix.aql.translator;

import edu.uci.ics.asterix.aql.base.Statement.Kind;
import edu.uci.ics.asterix.aql.expression.Query;
import edu.uci.ics.asterix.common.parse.IParseFileSplitsDecl;
import edu.uci.ics.asterix.translator.AbstractAqlTranslator;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;

public class DdlTranslator extends AbstractAqlTranslator {

    

    public static interface ICompiledStatement {

        public abstract Kind getKind();
    }

    public static class CompiledLoadFromFileStatement implements ICompiledStatement, IParseFileSplitsDecl {
        private String datasetName;
        private FileSplit[] splits;
        private boolean alreadySorted;
        private Character delimChar;

        public CompiledLoadFromFileStatement(String datasetName, FileSplit[] splits, Character delimChar,
                boolean alreadySorted) {
            this.datasetName = datasetName;
            this.splits = splits;
            this.delimChar = delimChar;
            this.alreadySorted = alreadySorted;
        }

        public String getDatasetName() {
            return datasetName;
        }

        @Override
        public FileSplit[] getSplits() {
            return splits;
        }

        @Override
        public Character getDelimChar() {
            return delimChar;
        }

        public boolean alreadySorted() {
            return alreadySorted;
        }

        @Override
        public boolean isDelimitedFileFormat() {
            return delimChar != null;
        }

        @Override
        public Kind getKind() {
            return Kind.LOAD_FROM_FILE;
        }
    }

    public static class CompiledWriteFromQueryResultStatement implements ICompiledStatement {

        private String dataverseName;
        private String datasetName;
        private Query query;
        private int varCounter;

        public CompiledWriteFromQueryResultStatement(String dataverseName, String datasetName, Query query,
                int varCounter) {
            this.dataverseName = dataverseName;
            this.datasetName = datasetName;
            this.query = query;
            this.varCounter = varCounter;
        }

        public String getDataverseName() {
            return dataverseName;
        }

        public String getDatasetName() {
            return datasetName;
        }

        public int getVarCounter() {
            return varCounter;
        }

        public Query getQuery() {
            return query;
        }

        @Override
        public Kind getKind() {
            return Kind.WRITE_FROM_QUERY_RESULT;
        }

    }

    public static class CompiledDatasetDropStatement implements ICompiledStatement {
        private final String dataverseName;
        private final String datasetName;

        public CompiledDatasetDropStatement(String dataverseName, String datasetName) {
            this.dataverseName = dataverseName;
            this.datasetName = datasetName;
        }

        public String getDataverseName() {
            return dataverseName;
        }

        public String getDatasetName() {
            return datasetName;
        }

        @Override
        public Kind getKind() {
            return Kind.DATASET_DROP;
        }
    }

    // added by yasser
    public static class CompiledCreateDataverseStatement implements ICompiledStatement {
        private String dataverseName;
        private String format;

        public CompiledCreateDataverseStatement(String dataverseName, String format) {
            this.dataverseName = dataverseName;
            this.format = format;
        }

        public String getDataverseName() {
            return dataverseName;
        }

        public String getFormat() {
            return format;
        }

        @Override
        public Kind getKind() {
            return Kind.CREATE_DATAVERSE;
        }
    }

    public static class CompiledNodeGroupDropStatement implements ICompiledStatement {
        private String nodeGroupName;

        public CompiledNodeGroupDropStatement(String nodeGroupName) {
            this.nodeGroupName = nodeGroupName;
        }

        public String getNodeGroupName() {
            return nodeGroupName;
        }

        @Override
        public Kind getKind() {
            return Kind.NODEGROUP_DROP;
        }
    }

    public static class CompiledIndexDropStatement implements ICompiledStatement {
        private String dataverseName;
        private String datasetName;
        private String indexName;

        public CompiledIndexDropStatement(String dataverseName, String datasetName, String indexName) {
            this.dataverseName = dataverseName;
            this.datasetName = datasetName;
            this.indexName = indexName;
        }

        public String getDataverseName() {
            return dataverseName;
        }

        public String getDatasetName() {
            return datasetName;
        }

        public String getIndexName() {
            return indexName;
        }

        @Override
        public Kind getKind() {
            return Kind.INDEX_DROP;
        }
    }

    public static class CompiledDataverseDropStatement implements ICompiledStatement {
        private String dataverseName;
        private boolean ifExists;

        public CompiledDataverseDropStatement(String dataverseName, boolean ifExists) {
            this.dataverseName = dataverseName;
            this.ifExists = ifExists;
        }

        public String getDataverseName() {
            return dataverseName;
        }

        public boolean getIfExists() {
            return ifExists;
        }

        @Override
        public Kind getKind() {
            return Kind.DATAVERSE_DROP;
        }
    }

    public static class CompiledTypeDropStatement implements ICompiledStatement {
        private String typeName;

        public CompiledTypeDropStatement(String nodeGroupName) {
            this.typeName = nodeGroupName;
        }

        public String getTypeName() {
            return typeName;
        }

        @Override
        public Kind getKind() {
            return Kind.TYPE_DROP;
        }
    }
}

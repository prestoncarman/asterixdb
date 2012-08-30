package edu.uci.ics.asterix.translator;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import edu.uci.ics.asterix.aql.base.Clause;
import edu.uci.ics.asterix.aql.base.Expression;
import edu.uci.ics.asterix.aql.base.Statement;
import edu.uci.ics.asterix.aql.base.Statement.Kind;
import edu.uci.ics.asterix.aql.expression.BeginFeedStatement;
import edu.uci.ics.asterix.aql.expression.CallExpr;
import edu.uci.ics.asterix.aql.expression.ControlFeedStatement;
import edu.uci.ics.asterix.aql.expression.ControlFeedStatement.OperationType;
import edu.uci.ics.asterix.aql.expression.CreateIndexStatement;
import edu.uci.ics.asterix.aql.expression.DeleteStatement;
import edu.uci.ics.asterix.aql.expression.FLWOGRExpression;
import edu.uci.ics.asterix.aql.expression.FieldAccessor;
import edu.uci.ics.asterix.aql.expression.FieldBinding;
import edu.uci.ics.asterix.aql.expression.ForClause;
import edu.uci.ics.asterix.aql.expression.Identifier;
import edu.uci.ics.asterix.aql.expression.InsertStatement;
import edu.uci.ics.asterix.aql.expression.LiteralExpr;
import edu.uci.ics.asterix.aql.expression.LoadFromFileStatement;
import edu.uci.ics.asterix.aql.expression.Query;
import edu.uci.ics.asterix.aql.expression.RecordConstructor;
import edu.uci.ics.asterix.aql.expression.VariableExpr;
import edu.uci.ics.asterix.aql.expression.WhereClause;
import edu.uci.ics.asterix.aql.expression.WriteFromQueryResultStatement;
import edu.uci.ics.asterix.aql.literal.StringLiteral;
import edu.uci.ics.asterix.common.config.DatasetConfig.DatasetType;
import edu.uci.ics.asterix.common.config.DatasetConfig.IndexType;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.common.functions.FunctionConstants;
import edu.uci.ics.asterix.common.functions.FunctionSignature;
import edu.uci.ics.asterix.metadata.IDatasetDetails;
import edu.uci.ics.asterix.metadata.MetadataException;
import edu.uci.ics.asterix.metadata.MetadataManager;
import edu.uci.ics.asterix.metadata.MetadataTransactionContext;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledMetadataDeclarations;
import edu.uci.ics.asterix.metadata.entities.Dataset;
import edu.uci.ics.asterix.metadata.entities.Index;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.transaction.management.exception.ACIDException;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;

public class DmlTranslator extends AbstractAqlTranslator {

    private final MetadataTransactionContext mdTxnCtx;
    private final List<Statement> aqlStatements;
    private AqlCompiledMetadataDeclarations compiledDeclarations;
    private List<ICompiledDmlStatement> compiledDmlStatements;

    public DmlTranslator(MetadataTransactionContext mdTxnCtx, List<Statement> aqlStatements) {
        this.mdTxnCtx = mdTxnCtx;
        this.aqlStatements = aqlStatements;
    }

    public void translate() throws AsterixException, RemoteException, ACIDException, MetadataException {
        compiledDeclarations = compileMetadata(mdTxnCtx, aqlStatements, true);
        compiledDmlStatements = compileDmlStatements();
    }

    public AqlCompiledMetadataDeclarations getCompiledDeclarations() {
        return compiledDeclarations;
    }

    public List<ICompiledDmlStatement> getCompiledDmlStatements() {
        return compiledDmlStatements;
    }

    private List<ICompiledDmlStatement> compileDmlStatements() throws AsterixException, MetadataException {
        List<ICompiledDmlStatement> dmlStatements = new ArrayList<ICompiledDmlStatement>();
        for (Statement stmt : aqlStatements) {
            validateOperation(compiledDeclarations, stmt);
            switch (stmt.getKind()) {
                case LOAD_FROM_FILE: {
                    LoadFromFileStatement loadStmt = (LoadFromFileStatement) stmt;
                    String dataverseName = loadStmt.getDataverseName() == null ? compiledDeclarations
                            .getDefaultDataverseName() : loadStmt.getDataverseName().getValue();
                    CompiledLoadFromFileStatement cls = new CompiledLoadFromFileStatement(dataverseName, loadStmt
                            .getDatasetName().getValue(), loadStmt.getAdapter(), loadStmt.getProperties(),
                            loadStmt.dataIsAlreadySorted());
                    dmlStatements.add(cls);
                    // Also load the dataset's secondary indexes.
                    List<Index> datasetIndexes = MetadataManager.INSTANCE.getDatasetIndexes(mdTxnCtx, dataverseName,
                            loadStmt.getDatasetName().getValue());
                    for (Index index : datasetIndexes) {
                        if (!index.isSecondaryIndex()) {
                            continue;
                        }
                        // Create CompiledCreateIndexStatement from metadata entity 'index'.
                        CompiledCreateIndexStatement cis = new CompiledCreateIndexStatement(index.getIndexName(),
                                dataverseName, index.getDatasetName(), index.getKeyFieldNames(), index.getGramLength(),
                                index.getIndexType());
                        dmlStatements.add(cis);
                    }
                    break;
                }
                case WRITE_FROM_QUERY_RESULT: {
                    WriteFromQueryResultStatement st1 = (WriteFromQueryResultStatement) stmt;
                    String dataverseName = st1.getDataverseName() == null ? compiledDeclarations
                            .getDefaultDataverseName() : st1.getDataverseName().getValue();
                    CompiledWriteFromQueryResultStatement clfrqs = new CompiledWriteFromQueryResultStatement(
                            dataverseName, st1.getDatasetName().getValue(), st1.getQuery(), st1.getVarCounter());
                    dmlStatements.add(clfrqs);
                    break;
                }
                case CREATE_INDEX: {
                    CreateIndexStatement cis = (CreateIndexStatement) stmt;
                    // Assumptions: We first processed the DDL, which added the secondary index to the metadata.
                    // If the index's dataset is being loaded in this 'session', then let the load add 
                    // the CompiledCreateIndexStatement to dmlStatements, and don't add it again here.
                    // It's better to have the load handle this because:
                    // 1. There may be more secondary indexes to load, which were possibly created in an earlier session.
                    // 2. If the create index stmt came before the load stmt, then we would first create an empty index only to load it again later. 
                    // This may cause problems because the index would be considered loaded (even though it was loaded empty). 
                    String loadDatasetStmtDataverse = null;
                    String ciStmtDataverse = null;
                    for (Statement s : aqlStatements) {
                        if (s.getKind() != Kind.LOAD_FROM_FILE) {
                            continue;
                        }
                        LoadFromFileStatement loadStmt = (LoadFromFileStatement) s;
                        loadDatasetStmtDataverse = loadStmt.getDataverseName() == null ? compiledDeclarations
                                .getDefaultDataverseName() : loadStmt.getDataverseName().getValue();
                        ciStmtDataverse = loadStmt.getDataverseName() == null ? compiledDeclarations
                                .getDefaultDataverseName() : loadStmt.getDataverseName().getValue();

                        if (loadStmt.getDatasetName().equals(cis.getDatasetName())
                                && loadDatasetStmtDataverse.equals(ciStmtDataverse)) {
                            cis.setNeedToCreate(false);
                        }
                    }
                    if (cis.getNeedToCreate()) {
                        CompiledCreateIndexStatement ccis = new CompiledCreateIndexStatement(cis.getIndexName()
                                .getValue(), ciStmtDataverse, cis.getDatasetName().getValue(), cis.getFieldExprs(),
                                cis.getGramLength(), cis.getIndexType());
                        dmlStatements.add(ccis);
                    }
                    break;
                }
                case INSERT: {
                    InsertStatement is = (InsertStatement) stmt;
                    String dataverseName = is.getDataverseName() == null ? compiledDeclarations
                            .getDefaultDataverseName() : is.getDataverseName().getValue();
                    CompiledInsertStatement clfrqs = new CompiledInsertStatement(dataverseName, is.getDatasetName()
                            .getValue(), is.getQuery(), is.getVarCounter());
                    dmlStatements.add(clfrqs);
                    break;
                }
                case DELETE: {
                    DeleteStatement ds = (DeleteStatement) stmt;
                    String dataverseName = ds.getDataverseName() == null ? compiledDeclarations
                            .getDefaultDataverseName() : ds.getDataverseName().getValue();

                    CompiledDeleteStatement clfrqs = new CompiledDeleteStatement(ds.getVariableExpr(), dataverseName,
                            ds.getDatasetName().getValue(), ds.getCondition(), ds.getDieClause(), ds.getVarCounter(),
                            compiledDeclarations);
                    dmlStatements.add(clfrqs);
                    break;
                }

                case BEGIN_FEED: {
                    BeginFeedStatement bfs = (BeginFeedStatement) stmt;
                    String dataverseName = bfs.getDataverseName() == null ? compiledDeclarations
                            .getDefaultDataverseName() : bfs.getDataverseName().getValue();

                    CompiledBeginFeedStatement cbfs = new CompiledBeginFeedStatement(dataverseName, bfs
                            .getDatasetName().getValue(), bfs.getQuery(), bfs.getVarCounter());
                    dmlStatements.add(cbfs);
                    Dataset dataset;
                    dataset = MetadataManager.INSTANCE.getDataset(mdTxnCtx,
                            compiledDeclarations.getDefaultDataverseName(), bfs.getDatasetName().getValue());
                    IDatasetDetails datasetDetails = dataset.getDatasetDetails();
                    if (datasetDetails.getDatasetType() != DatasetType.FEED) {
                        throw new IllegalArgumentException("Dataset " + bfs.getDatasetName().getValue()
                                + " is not a feed dataset");
                    }
                    bfs.initialize(mdTxnCtx, dataset);
                    cbfs.setQuery(bfs.getQuery());
                    break;
                }

                case CONTROL_FEED: {
                    ControlFeedStatement cfs = (ControlFeedStatement) stmt;
                    CompiledControlFeedStatement clcfs = new CompiledControlFeedStatement(cfs.getOperationType(), cfs
                            .getDataverseName().getValue(), cfs.getDatasetName().getValue(),
                            cfs.getAlterAdapterConfParams());
                    dmlStatements.add(clcfs);
                    break;

                }
            }
        }
        return dmlStatements;
    }

    public static interface ICompiledDmlStatement {

        public abstract Kind getKind();

        public String getDataverseName();

        public String getDatasetName();
    }

    public static class CompiledCreateIndexStatement implements ICompiledDmlStatement {
        private final String indexName;
        private final String dataverseName;
        private final String datasetName;
        private final List<String> keyFields;
        private final IndexType indexType;

        // Specific to NGram index.
        private final int gramLength;

        public CompiledCreateIndexStatement(String indexName, String dataverseName, String datasetName,
                List<String> keyFields, int gramLength, IndexType indexType) {
            this.indexName = indexName;
            this.dataverseName = dataverseName;
            this.datasetName = datasetName;
            this.keyFields = keyFields;
            this.gramLength = gramLength;
            this.indexType = indexType;
        }

        public String getDatasetName() {
            return datasetName;
        }

        public String getDataverseName() {
            return dataverseName;
        }

        public String getIndexName() {
            return indexName;
        }

        public List<String> getKeyFields() {
            return keyFields;
        }

        public IndexType getIndexType() {
            return indexType;
        }

        public int getGramLength() {
            return gramLength;
        }

        @Override
        public Kind getKind() {
            return Kind.CREATE_INDEX;
        }
    }

    public static class CompiledLoadFromFileStatement implements ICompiledDmlStatement {
        private String dataverseName;
        private String datasetName;
        private boolean alreadySorted;
        private String adapter;
        private Map<String, String> properties;

        public CompiledLoadFromFileStatement(String dataverseName, String datasetName, String adapter,
                Map<String, String> properties, boolean alreadySorted) {
            this.dataverseName = dataverseName;
            this.datasetName = datasetName;
            this.alreadySorted = alreadySorted;
            this.adapter = adapter;
            this.properties = properties;
        }

        public String getDataverseName() {
            return dataverseName;
        }

        public String getDatasetName() {
            return datasetName;
        }

        public boolean alreadySorted() {
            return alreadySorted;
        }

        public String getAdapter() {
            return adapter;
        }

        public Map<String, String> getProperties() {
            return properties;
        }

        @Override
        public Kind getKind() {
            return Kind.LOAD_FROM_FILE;
        }
    }

    public static class CompiledWriteFromQueryResultStatement implements ICompiledDmlStatement {

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

        @Override
        public String getDatasetName() {
            return datasetName;
        }

        @Override
        public String getDataverseName() {
            return dataverseName;
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

    public static class CompiledInsertStatement implements ICompiledDmlStatement {
        private final String dataverseName;
        private final String datasetName;
        private final Query query;
        private final int varCounter;

        public CompiledInsertStatement(String dataverseName, String datasetName, Query query, int varCounter) {
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
            return Kind.INSERT;
        }
    }

    public static class CompiledBeginFeedStatement implements ICompiledDmlStatement {
        private String dataverseName;
        private String datasetName;
        private Query query;
        private int varCounter;

        public CompiledBeginFeedStatement(String dataverseName, String datasetName, Query query, int varCounter) {
            this.dataverseName = dataverseName;
            this.datasetName = datasetName;
            this.query = query;
            this.varCounter = varCounter;
        }

        @Override
        public String getDataverseName() {
            return dataverseName;
        }

        @Override
        public String getDatasetName() {
            return datasetName;
        }

        public int getVarCounter() {
            return varCounter;
        }

        public Query getQuery() {
            return query;
        }

        public void setQuery(Query query) {
            this.query = query;
        }

        @Override
        public Kind getKind() {
            return Kind.BEGIN_FEED;
        }
    }

    public static class CompiledControlFeedStatement implements ICompiledDmlStatement {
        private String dataverseName;
        private String datasetName;
        private OperationType operationType;
        private Query query;
        private int varCounter;
        private Map<String, String> alteredParams;

        public CompiledControlFeedStatement(OperationType operationType, String dataverseName, String datasetName,
                Map<String, String> alteredParams) {
            this.dataverseName = dataverseName;
            this.datasetName = datasetName;
            this.operationType = operationType;
            this.alteredParams = alteredParams;
        }

        @Override
        public String getDataverseName() {
            return dataverseName;
        }

        @Override
        public String getDatasetName() {
            return datasetName;
        }

        public OperationType getOperationType() {
            return operationType;
        }

        public int getVarCounter() {
            return varCounter;
        }

        public Query getQuery() {
            return query;
        }

        @Override
        public Kind getKind() {
            return Kind.CONTROL_FEED;
        }

        public Map<String, String> getProperties() {
            return alteredParams;
        }

        public void setProperties(Map<String, String> properties) {
            this.alteredParams = properties;
        }
    }

    public static class CompiledDeleteStatement implements ICompiledDmlStatement {
        private VariableExpr var;
        private String dataverseName;
        private String datasetName;
        private Expression condition;
        private Clause dieClause;
        private int varCounter;
        private AqlCompiledMetadataDeclarations compiledDeclarations;

        public CompiledDeleteStatement(VariableExpr var, String dataverseName, String datasetName,
                Expression condition, Clause dieClause, int varCounter,
                AqlCompiledMetadataDeclarations compiledDeclarations) {
            this.var = var;
            this.dataverseName = dataverseName;
            this.datasetName = datasetName;
            this.condition = condition;
            this.dieClause = dieClause;
            this.varCounter = varCounter;
            this.compiledDeclarations = compiledDeclarations;
        }

        @Override
        public String getDatasetName() {
            return datasetName;
        }

        @Override
        public String getDataverseName() {
            return dataverseName;
        }

        public int getVarCounter() {
            return varCounter;
        }

        public Expression getCondition() {
            return condition;
        }

        public Clause getDieClause() {
            return dieClause;
        }

        public Query getQuery(String dataverse) throws AlgebricksException {

            List<Expression> arguments = new ArrayList<Expression>();
            LiteralExpr argumentLiteral = new LiteralExpr(new StringLiteral(datasetName));
            arguments.add(argumentLiteral);

            CallExpr callExpression = new CallExpr(new FunctionSignature(FunctionConstants.ASTERIX_NS, "dataset", 1),
                    arguments);
            List<Clause> clauseList = new ArrayList<Clause>();
            Clause forClause = new ForClause(var, callExpression);
            clauseList.add(forClause);
            Clause whereClause = null;
            if (condition != null) {
                whereClause = new WhereClause(condition);
                clauseList.add(whereClause);
            }
            if (dieClause != null) {
                clauseList.add(dieClause);
            }

            Dataset dataset = compiledDeclarations.findDataset(dataverse, datasetName);
            if (dataset == null) {
                throw new AlgebricksException("Unknown dataset " + datasetName);
            }
            String itemTypeName = dataset.getItemTypeName();
            IAType itemType = compiledDeclarations.findType(dataset.getDataverseName(), itemTypeName);
            ARecordType recType = (ARecordType) itemType;
            String[] fieldNames = recType.getFieldNames();
            List<FieldBinding> fieldBindings = new ArrayList<FieldBinding>();
            for (int i = 0; i < fieldNames.length; i++) {
                FieldAccessor fa = new FieldAccessor(var, new Identifier(fieldNames[i]));
                FieldBinding fb = new FieldBinding(new LiteralExpr(new StringLiteral(fieldNames[i])), fa);
                fieldBindings.add(fb);
            }
            RecordConstructor rc = new RecordConstructor(fieldBindings);

            FLWOGRExpression flowgr = new FLWOGRExpression(clauseList, rc);
            Query query = new Query();
            query.setBody(flowgr);
            return query;
        }

        @Override
        public Kind getKind() {
            return Kind.DELETE;
        }

    }

}

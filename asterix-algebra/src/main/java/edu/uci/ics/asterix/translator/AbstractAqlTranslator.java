package edu.uci.ics.asterix.translator;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.uci.ics.asterix.aql.base.Statement;
import edu.uci.ics.asterix.aql.expression.DataverseDecl;
import edu.uci.ics.asterix.aql.expression.DataverseDropStatement;
import edu.uci.ics.asterix.aql.expression.DropStatement;
import edu.uci.ics.asterix.aql.expression.FunctionDropStatement;
import edu.uci.ics.asterix.aql.expression.NodeGroupDropStatement;
import edu.uci.ics.asterix.aql.expression.SetStatement;
import edu.uci.ics.asterix.aql.expression.TypeDecl;
import edu.uci.ics.asterix.aql.expression.WriteStatement;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.common.functions.FunctionSignature;
import edu.uci.ics.asterix.metadata.MetadataException;
import edu.uci.ics.asterix.metadata.MetadataTransactionContext;
import edu.uci.ics.asterix.metadata.bootstrap.MetadataConstants;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledMetadataDeclarations;
import edu.uci.ics.asterix.metadata.entities.AsterixBuiltinArtifactMap;
import edu.uci.ics.asterix.metadata.entities.AsterixBuiltinArtifactMap.ARTIFACT_KIND;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.data.IAWriterFactory;
import edu.uci.ics.hyracks.algebricks.runtime.writers.PrinterBasedWriterFactory;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;

public abstract class AbstractAqlTranslator {

    public AqlCompiledMetadataDeclarations compileMetadata(MetadataTransactionContext mdTxnCtx,
            List<Statement> statements, boolean online) throws AsterixException, MetadataException {
        List<TypeDecl> typeDeclarations = new ArrayList<TypeDecl>();
        Map<String, String> config = new HashMap<String, String>();

        FileSplit outputFile = null;
        IAWriterFactory writerFactory = null;
        String defaultDataverse = null;
        for (Statement stmt : statements) {
            switch (stmt.getKind()) {
                case TYPE_DECL: {
                    if (defaultDataverse == null && ((TypeDecl) stmt).getDataverseName() == null) {
                        throw new AsterixException(" Dataverse not specified for type:"
                                + ((TypeDecl) stmt).getIdent().getValue());
                    }
                    typeDeclarations.add((TypeDecl) stmt);
                    break;
                }
                case DATAVERSE_DECL: {
                    DataverseDecl dstmt = (DataverseDecl) stmt;
                    defaultDataverse = dstmt.getDataverseName().toString();
                    break;
                }
                case WRITE: {
                    if (outputFile != null) {
                        throw new AsterixException("Multiple 'write' statements.");
                    }
                    WriteStatement ws = (WriteStatement) stmt;
                    File f = new File(ws.getFileName());
                    outputFile = new FileSplit(ws.getNcName().getValue(), new FileReference(f));
                    if (ws.getWriterClassName() != null) {
                        try {
                            writerFactory = (IAWriterFactory) Class.forName(ws.getWriterClassName()).newInstance();
                        } catch (Exception e) {
                            throw new AsterixException(e);
                        }
                    }
                    break;
                }
                case SET: {
                    SetStatement ss = (SetStatement) stmt;
                    String pname = ss.getPropName();
                    String pvalue = ss.getPropValue();
                    config.put(pname, pvalue);
                    break;
                }
            }
        }
        if (writerFactory == null) {
            writerFactory = PrinterBasedWriterFactory.INSTANCE;
        }

        MetadataDeclTranslator metadataTranslator = new MetadataDeclTranslator(mdTxnCtx, defaultDataverse, outputFile,
                writerFactory, config, typeDeclarations);
        return metadataTranslator.computeMetadataDeclarations(online);
    }

    public void validateOperation(AqlCompiledMetadataDeclarations compiledDeclarations, Statement stmt)
            throws AsterixException {
        if (compiledDeclarations.getDefaultDataverseName() != null
                && compiledDeclarations.getDefaultDataverseName().equals(MetadataConstants.METADATA_DATAVERSE_NAME)) {

            boolean invalidOperation = false;
            String message = null;
            switch (stmt.getKind()) {
                case INSERT:
                case UPDATE:
                case DELETE:
                    invalidOperation = true;
                    message = " Operation  " + stmt.getKind() + " not permitted in system dataverse-"
                            + MetadataConstants.METADATA_DATAVERSE_NAME;
                    break;
                case FUNCTION_DROP:
                    FunctionSignature signature = ((FunctionDropStatement) stmt).getFunctionSignature();
                    FunctionIdentifier fId = new FunctionIdentifier(signature.getNamespace(), signature.getName(),
                            signature.getArity());
                    if (compiledDeclarations.getDefaultDataverseName()
                            .equals(MetadataConstants.METADATA_DATAVERSE_NAME)
                            && AsterixBuiltinArtifactMap.isSystemProtectedArtifact(ARTIFACT_KIND.FUNCTION, fId)) {
                        invalidOperation = true;
                        message = "Invalid Operation cannot drop function " + signature + " (protected by system)";
                    }
                    break;
                case NODEGROUP_DROP:
                    NodeGroupDropStatement nodeGroupDropStmt = (NodeGroupDropStatement) stmt;
                    String nodegroupName = nodeGroupDropStmt.getNodeGroupName().getValue();
                    if (AsterixBuiltinArtifactMap.isSystemProtectedArtifact(ARTIFACT_KIND.NODEGROUP, nodegroupName)) {
                        message = "Invalid Operation cannot drop nodegroup " + nodegroupName + " (protected by system)";
                        invalidOperation = true;
                    }
                    break;
                case DATAVERSE_DROP:
                    DataverseDropStatement dvDropStmt = (DataverseDropStatement) stmt;
                    String dvName = dvDropStmt.getDataverseName().getValue();
                    if (dvName.equals(MetadataConstants.METADATA_DATAVERSE_NAME)) {
                        message = "Invalid Operation cannot drop dataverse " + dvName + " (protected by system)";
                        invalidOperation = true;
                    }
                    break;
                case DATASET_DROP:
                    DropStatement dropStmt = (DropStatement) stmt;
                    String datasetName = dropStmt.getDatasetName().getValue();
                    if (AsterixBuiltinArtifactMap.isSystemProtectedArtifact(ARTIFACT_KIND.DATASET, datasetName)) {
                        invalidOperation = true;
                        message = "Invalid Operation cannot drop dataset " + datasetName + " (protected by system)";
                    }
                    break;
            }
            if (invalidOperation) {
                throw new AsterixException(message);
            }
        }
    }
}
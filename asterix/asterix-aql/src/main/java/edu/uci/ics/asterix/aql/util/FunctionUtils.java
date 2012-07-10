package edu.uci.ics.asterix.aql.util;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.asterix.aql.expression.FunctionDecl;
import edu.uci.ics.asterix.aql.expression.Query;
import edu.uci.ics.asterix.aql.expression.VarIdentifier;
import edu.uci.ics.asterix.aql.parser.AQLParser;
import edu.uci.ics.asterix.aql.parser.ParseException;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.common.functions.FunctionConstants;
import edu.uci.ics.asterix.metadata.MetadataException;
import edu.uci.ics.asterix.metadata.MetadataManager;
import edu.uci.ics.asterix.metadata.MetadataTransactionContext;
import edu.uci.ics.asterix.metadata.entities.Function;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.functions.AsterixFunction;
import edu.uci.ics.asterix.om.functions.AsterixFunctionInfo;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.IFunctionInfo;

public class FunctionUtils {

    public static FunctionDecl getFunctionDecl(Function function) throws AsterixException {
        String functionBody = function.getFunctionBody();
        List<String> params = function.getParams();
        List<VarIdentifier> varIdentifiers = new ArrayList<VarIdentifier>();

        StringBuilder builder = new StringBuilder();
        builder.append(" declare function " + function.getFunctionName());
        builder.append("(");
        for (String param : params) {
            VarIdentifier varId = new VarIdentifier(param);
            varIdentifiers.add(varId);
            builder.append(param);
            builder.append(",");
        }
        if (params.size() > 0) {
            builder.delete(builder.length() - 1, builder.length());
        }
        builder.append(")");
        builder.append("{");
        builder.append(functionBody);
        builder.append("}");
        AQLParser parser = new AQLParser(new StringReader(new String(builder)));

        Query query = null;
        try {
            query = (Query) parser.Statement();
        } catch (ParseException pe) {
            throw new AsterixException(pe);
        }

        FunctionDecl decl = (FunctionDecl) query.getPrologDeclList().get(0);
        return decl;
    }
    
    public static IFunctionInfo getFunctionInfo(FunctionIdentifier fi) {
        return AsterixBuiltinFunctions.getAsterixFunctionInfo(fi);
    }

    public static IFunctionInfo getFunctionInfo(MetadataTransactionContext mdTxnCtx, String dataverseName,
            AsterixFunction asterixFunction) throws MetadataException {
        FunctionIdentifier fid = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
                asterixFunction.getFunctionName(), asterixFunction.getArity(), true);
        IFunctionInfo finfo = AsterixBuiltinFunctions.getAsterixFunctionInfo(fid);
        if (fid == null) {
            fid = new FunctionIdentifier(AlgebricksBuiltinFunctions.ALGEBRICKS_NS, asterixFunction.getFunctionName(),
                    asterixFunction.getArity(), true);
        }
        if (fid == null) {
            Function function = MetadataManager.INSTANCE.getFunction(mdTxnCtx, dataverseName,
                    asterixFunction.getFunctionName(), asterixFunction.getArity());
            if (function != null) {
                finfo = new AsterixFunctionInfo(dataverseName, asterixFunction, false);
                // todo: for external functions, we shall construct another kind
                // of function info (that extends AsterixFunctionInfo)
                // and has additional information.
            }
        }
        return finfo; // could be null
    }
}

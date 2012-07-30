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
import edu.uci.ics.asterix.metadata.entities.Function;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
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

    /*
    public static IFunctionInfo getFunctionInfo(MetadataTransactionContext mdTxnCtx, String dataverseName,
            AsterixFunction asterixFunction) throws MetadataException {
        FunctionIdentifier fid = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
                asterixFunction.getFunctionName(), asterixFunction.getArity());
        IFunctionInfo finfo = AsterixBuiltinFunctions.getAsterixFunctionInfo(fid);
        if (fid == null) {
            fid = new FunctionIdentifier(AlgebricksBuiltinFunctions.ALGEBRICKS_NS, asterixFunction.getFunctionName(),
                    asterixFunction.getArity());
        }
        if (fid == null) {
            Function function = MetadataManager.INSTANCE.getFunction(mdTxnCtx, dataverseName,
                    asterixFunction.getFunctionName(), asterixFunction.getArity());
            if (function != null) {
                if (function.getLanguage().equalsIgnoreCase(Function.LANGUAGE_AQL)) {
                    finfo = new AsterixFunctionInfo(dataverseName, asterixFunction, false);
                } else if (function.getLanguage().equalsIgnoreCase(Function.LANGUAGE_JAVA)) {
                    finfo = getExternalFunctionInfo(mdTxnCtx, function);
                } else {
                    throw new MetadataException(" external function in " + function.getLanguage()
                            + " language  are not supported");
                }
            }
        }
        return finfo; // could be null
    }

    private static IFunctionInfo getExternalFunctionInfo(MetadataTransactionContext mdTxnCtx, Function function)
            throws MetadataException {
        switch (FunctionKind.valueOf(function.getFunctionKind())) {
            case SCALAR:
                final IAType type;
                String returnType = function.getReturnType();
                BuiltinType builtinType = AsterixBuiltinTypeMap.getBuiltinTypes().get(returnType);
                if (builtinType != null) {
                    type = builtinType.getType();
                } else {
                    Datatype datatype = MetadataManager.INSTANCE.getDatatype(mdTxnCtx, function.getDataverseName(),
                            returnType);
                    if (datatype != null) {
                        type = datatype.getDatatype();
                    } else {
                        throw new MetadataException(" Unknown return type :" + function.getReturnType());
                    }
                }
                AsterixExternalScalarFunctionInfo asInfo = new AsterixExternalScalarFunctionInfo(
                        function.getDataverseName(), new AsterixFunction(function.getFunctionName(), function
                                .getParams().size()), FunctionKind.SCALAR, function.getFunctionBody(),
                        function.getLanguage(), new IResultTypeComputer() {

                            @Override
                            public IAType computeType(ILogicalExpression expression, IVariableTypeEnvironment env,
                                    IMetadataProvider<?, ?> metadataProvider) throws AlgebricksException {
                                return type;
                            }
                        });
                return asInfo;
            case AGGREGATE:
            case UNNEST:
            case STATEFUL:
                throw new MetadataException(" function of kind :" + function.getFunctionKind() + " are not supported");
            default:
                throw new MetadataException(" Unknown function kind :" + function.getFunctionKind());
        }
    }*/

}

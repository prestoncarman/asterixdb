package edu.uci.ics.asterix.om.functions;

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
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;

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
import edu.uci.ics.asterix.om.functions.AsterixRegisteredFunctions;
import edu.uci.ics.asterix.om.typecomputer.base.IResultTypeComputer;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.IFunctionInfo;

public class FunctionUtils {

   
    public static IFunctionInfo getFunctionInfo(FunctionIdentifier fi) {
        return AsterixBuiltinFunctions.getAsterixFunctionInfo(fi);
    }

    public static IFunctionInfo getFunctionInfo(MetadataTransactionContext mdTxnCtx, String dataverseName,
            AsterixFunction asterixFunction) throws MetadataException {
        FunctionIdentifier fid = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
                asterixFunction.getFunctionName(), asterixFunction.getArity(), true);
        IFunctionInfo finfo = AsterixBuiltinFunctions.getAsterixFunctionInfo(fid);
        if (finfo == null) {
            fid = new FunctionIdentifier(AlgebricksBuiltinFunctions.ALGEBRICKS_NS, asterixFunction.getFunctionName(),
                    asterixFunction.getArity(), true);
            finfo = AsterixBuiltinFunctions.getAsterixFunctionInfo(fid);

            if (finfo == null) {
                Function function = MetadataManager.INSTANCE.getFunction(mdTxnCtx, dataverseName, asterixFunction
                        .getFunctionName(), asterixFunction.getArity());
                if (function != null) {
                    finfo = new AsterixFunctionInfo(dataverseName, asterixFunction, function.getFunctionKind(), false);
                }
            }
        }
        return finfo; // could be null
    }

    public static boolean isAggregateFunction(String dataverse, AsterixFunction asterixFunction) {
        FunctionIdentifier fid = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
                asterixFunction.getFunctionName(), asterixFunction.getArity(), true);
        if (AsterixBuiltinFunctions.isAggregateFunctionSerializable(fid)) {
            return true;
        }

        fid = new FunctionIdentifier(AlgebricksBuiltinFunctions.ALGEBRICKS_NS, asterixFunction.getFunctionName(),
                asterixFunction.getArity(), true);
        if (AsterixBuiltinFunctions.isAggregateFunctionSerializable(fid)) {
            return true;
        }

        return AsterixRegisteredFunctions.isAggregateFunction(fid);
    }

    public static boolean isUnnestingFunction(String dataverse, AsterixFunction asterixFunction) {
        FunctionIdentifier fid = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
                asterixFunction.getFunctionName(), asterixFunction.getArity(), true);
        if (AsterixBuiltinFunctions.isBuiltinUnnestingFunction(fid)) {
            return true;
        }

        fid = new FunctionIdentifier(AlgebricksBuiltinFunctions.ALGEBRICKS_NS, asterixFunction.getFunctionName(),
                asterixFunction.getArity(), true);
        if (AsterixBuiltinFunctions.isBuiltinUnnestingFunction(fid)) {
            return true;
        }

        return AsterixRegisteredFunctions.isUnnestingFunction(fid);
    }

    public static boolean isBuiltinUnnestingFunction(IFunctionInfo finfo) {
        if (finfo.getFunctionIdentifier().isBuiltin()) {
            return AsterixBuiltinFunctions.isBuiltinUnnestingFunction(finfo.getFunctionIdentifier());
        } else if (finfo instanceof AsterixFunctionInfo) {

        }
        return false;
    }

    public static IResultTypeComputer getResultTypeComputer(FunctionIdentifier fi) {
        if (fi.isBuiltin()) {
            return AsterixBuiltinFunctions.getResultTypeComputer(fi);
        } else {
            return null;
        }
    }

    public static FunctionIdentifier getAggregateFunction(FunctionIdentifier scalarVersionOfAggregate) {
        if (scalarVersionOfAggregate.isBuiltin()) {
            return AsterixBuiltinFunctions.getAggregateFunction(scalarVersionOfAggregate);
        } else {
            return AsterixRegisteredFunctions.getAggregateFunction(scalarVersionOfAggregate);
        }
    }

    public static AggregateFunctionCallExpression makeAggregateFunctionExpression(FunctionIdentifier fi,
            List<Mutable<ILogicalExpression>> args) {
        if (fi.isBuiltin()) {
            return AsterixBuiltinFunctions.makeAggregateFunctionExpression(fi, args);
        } else {
            return AsterixRegisteredFunctions.makeAggregateFunctionExpression(fi, args);
        }
    }

    public static AggregateFunctionCallExpression makeSerializableAggregateFunctionExpression(FunctionIdentifier fi,
            List<Mutable<ILogicalExpression>> args) {
        if (fi.isBuiltin()) {
            return AsterixBuiltinFunctions.makeSerializableAggregateFunctionExpression(fi, args);
        } else {
            return AsterixRegisteredFunctions.makeSerializableAggregateFunctionExpression(fi, args);
        }
    }

    public static boolean isDatasetFunction(FunctionIdentifier fi) {
        if (fi.isBuiltin()) {
            return AsterixBuiltinFunctions.isDatasetFunction(fi);
        } else {
            return AsterixRegisteredFunctions.isDatasetFunction(fi);
        }
    }

    public static boolean isBuiltinUnnestingFunction(FunctionIdentifier fi) {
        if (fi.isBuiltin()) {
            return AsterixBuiltinFunctions.isBuiltinUnnestingFunction(fi);
        } else {
            return false;
        }
    }

    public static boolean returnsUniqueValues(FunctionIdentifier fi) {
        if (fi.isBuiltin()) {
            return AsterixBuiltinFunctions.returnsUniqueValues(fi);
        } else {
            return false;
        }
    }

    public static boolean isBuiltinCompilerFunction(FunctionIdentifier fi) {
        return AsterixBuiltinFunctions.isBuiltinCompilerFunction(fi);
    }

}

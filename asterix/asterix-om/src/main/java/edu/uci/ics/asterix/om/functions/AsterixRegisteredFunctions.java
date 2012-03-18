package edu.uci.ics.asterix.om.functions;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.IFunctionInfo;

public class AsterixRegisteredFunctions {

    private final static Map<FunctionIdentifier, IFunctionInfo> asterixFunctionIdToInfo = new HashMap<FunctionIdentifier, IFunctionInfo>();

    public static FunctionIdentifier getAggregateFunction(FunctionIdentifier scalarVersionOfAggregate) {
        // TODO Auto-generated method stub
        return null;
    }

    public static AggregateFunctionCallExpression makeAggregateFunctionExpression(FunctionIdentifier fi,
            List<Mutable<ILogicalExpression>> args) {
        // TODO Auto-generated method stub
        return null;
    }

    public static AggregateFunctionCallExpression makeSerializableAggregateFunctionExpression(FunctionIdentifier fi,
            List<Mutable<ILogicalExpression>> args) {
        // TODO Auto-generated method stub
        return null;
    }

    public static boolean isDatasetFunction(FunctionIdentifier fi) {
        // TODO Auto-generated method stub
        return false;
    }

    public static boolean isAggregateFunction(FunctionIdentifier fid) {
        // TODO Auto-generated method stub
        return false;
    }

    public static boolean isUnnestingFunction(FunctionIdentifier fid) {
        // TODO Auto-generated method stub
        return false;
    }

}

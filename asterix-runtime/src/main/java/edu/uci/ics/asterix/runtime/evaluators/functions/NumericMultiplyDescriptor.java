package edu.uci.ics.asterix.runtime.evaluators.functions;

import edu.uci.ics.asterix.common.functions.FunctionConstants;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class NumericMultiplyDescriptor extends AbstractNumericArithmeticEval {

    private static final long serialVersionUID = 1L;
    public final static FunctionIdentifier FID = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "numeric-multiply", 2, true);

    @Override
    public FunctionIdentifier getIdentifier() {
        return FID;
    }

    @Override
    protected long evaluateInteger(long lhs, long rhs) throws HyracksDataException {
        // TODO Auto-generated method stub
        return lhs * rhs;
    }

    @Override
    protected double evaluateDouble(double lhs, double rhs) throws HyracksDataException {
        // TODO Auto-generated method stub
        return lhs * rhs;
    }
}

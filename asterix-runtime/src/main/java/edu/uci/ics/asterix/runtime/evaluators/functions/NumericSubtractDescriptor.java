package edu.uci.ics.asterix.runtime.evaluators.functions;

import edu.uci.ics.asterix.common.functions.FunctionConstants;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class NumericSubtractDescriptor extends AbstractNumericArithmeticEval {

    private static final long serialVersionUID = 1L;
    public final static FunctionIdentifier FID = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "numeric-subtract", 2, true);

    @Override
    public FunctionIdentifier getIdentifier() {
        return FID;
    }

    @Override
    protected long evaluateInteger(long x, long y) throws HyracksDataException {
        long z = x - y;  
        if (x > 0) {  
            if (y < 0 && z < 0)  
                throw new ArithmeticException("Overflow subtracting " + x + " + " + y);  
        } else if (y > 0 && z > 0)  
                throw new ArithmeticException("Overflow subtracting " + x + " + " + y);  
        return z; 
    }

    @Override
    protected double evaluateDouble(double lhs, double rhs) throws HyracksDataException {
        return lhs - rhs;
    }
}

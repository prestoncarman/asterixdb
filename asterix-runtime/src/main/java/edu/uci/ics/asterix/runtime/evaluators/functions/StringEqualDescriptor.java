package edu.uci.ics.asterix.runtime.evaluators.functions;

import edu.uci.ics.asterix.common.functions.FunctionConstants;
import edu.uci.ics.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.exceptions.NotImplementedException;
import edu.uci.ics.hyracks.algebricks.runtime.base.IEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.IEvaluatorFactory;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IDataOutputProvider;
import java.io.DataOutput;

/**
 *
 * @author Xiaoyu Ma
 */
public class StringEqualDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;

    private final static FunctionIdentifier FID = new FunctionIdentifier(FunctionConstants.ASTERIX_NS, "string-equal", 2,
            true);

    @Override
    public IEvaluatorFactory createEvaluatorFactory(final IEvaluatorFactory[] args) throws AlgebricksException {

        return new IEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IEvaluator createEvaluator(IDataOutputProvider output) throws AlgebricksException {

                DataOutput dout = output.getDataOutput();

                return new AbstractBinaryStringBoolEval(dout, args[0], args[1]) {

                    @Override
                    protected boolean compute(byte[] lBytes, int lLen, int lStart, 
                                        byte[] rBytes, int rLen, int rStart, 
                                        ArrayBackedValueStorage array0, ArrayBackedValueStorage array1) {
                        int len = UTF8StringPointable.getUTFLen(lBytes, 1);
                        
                        if(len != UTF8StringPointable.getUTFLen(rBytes, 1))
                            return false;
                        
                        int pos = 3;
                        while(pos < len + 3) {
                            char c1 = UTF8StringPointable.charAt(lBytes, pos);
                            char c2 = UTF8StringPointable.charAt(rBytes, pos);
                            if(c1 != c2)
                                return false;
                            
                            pos += UTF8StringPointable.charSize(lBytes, pos);
                        }
                        
                        return true;
                    }

                };
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return FID;
    }    
}

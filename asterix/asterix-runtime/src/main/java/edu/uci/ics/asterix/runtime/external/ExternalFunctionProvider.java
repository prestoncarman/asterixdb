package edu.uci.ics.asterix.runtime.external;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.om.functions.IExternalFunctionInfo;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IDataOutputProvider;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class ExternalFunctionProvider {

    public static IExternalFunction getExternalFunctionEvaluator(IExternalFunctionInfo finfo,
            ICopyEvaluatorFactory args[], IDataOutputProvider outputProvider) throws AlgebricksException {
        switch (finfo.getKind()) {
            case SCALAR:
                return new ExternalScalarFunction(finfo, args, outputProvider);
            case AGGREGATE:
            case UNNEST:
                throw new IllegalArgumentException(" not supported function kind" + finfo.getKind());
            default:
                throw new IllegalArgumentException(" unknown function kind" + finfo.getKind());
        }
    }
}

class ExternalScalarFunction extends ExternalFunction implements ICopyEvaluator {

    public ExternalScalarFunction(IExternalFunctionInfo finfo, ICopyEvaluatorFactory args[],
            IDataOutputProvider outputProvider) throws AlgebricksException {
        super(finfo, args, outputProvider);
    }

    @Override
    public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
        setArguments(tuple);
        try {
            evaluate(argumentProvider, resultCollector);
        } catch (Exception e) {
            throw new AlgebricksException(e);
        }
    }

    public void evaluate(IArgumentProvider argumentProvider, IResultCollector resultCollector) throws Exception {
        ((IExternalScalarFunction) externalFunction).evaluate(argumentProvider, resultCollector);
    }

}

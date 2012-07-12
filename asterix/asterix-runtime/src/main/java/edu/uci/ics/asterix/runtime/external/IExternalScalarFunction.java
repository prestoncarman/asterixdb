package edu.uci.ics.asterix.runtime.external;

public interface IExternalScalarFunction extends IExternalFunction {

    public void evaluate(IArgumentProvider argumentProvider, IResultCollector collector) throws Exception;

}

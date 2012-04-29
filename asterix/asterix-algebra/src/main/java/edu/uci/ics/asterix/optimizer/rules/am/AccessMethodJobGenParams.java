package edu.uci.ics.asterix.optimizer.rules.am;

import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;

public class AccessMethodJobGenParams {
    protected String indexName;
    protected String indexType;
    protected String datasetName;
    protected boolean retainInput;
    protected boolean requiresBroadcast;
    
    public AccessMethodJobGenParams() {
    }
    
    public AccessMethodJobGenParams(String indexName, String indexType, String datasetName, boolean retainInput, boolean requiresBroadcast) {
        this.indexName = indexName;
        this.indexType = indexType;
        this.datasetName = datasetName;
        this.retainInput = retainInput;
        this.requiresBroadcast = requiresBroadcast;
    }
    
    public void writeToFuncArgs(List<Mutable<ILogicalExpression>> funcArgs) {
        funcArgs.add(new MutableObject<ILogicalExpression>(AccessMethodUtils.createStringConstant(indexName)));
        funcArgs.add(new MutableObject<ILogicalExpression>(AccessMethodUtils.createStringConstant(indexType)));
        funcArgs.add(new MutableObject<ILogicalExpression>(AccessMethodUtils.createStringConstant(datasetName)));
        funcArgs.add(new MutableObject<ILogicalExpression>(AccessMethodUtils.createBooleanConstant(retainInput)));
        funcArgs.add(new MutableObject<ILogicalExpression>(AccessMethodUtils.createBooleanConstant(requiresBroadcast)));
    }

    public void readFromFuncArgs(List<Mutable<ILogicalExpression>> funcArgs) {
        indexName = AccessMethodUtils.getStringConstant(funcArgs.get(0));
        indexType = AccessMethodUtils.getStringConstant(funcArgs.get(1));
        datasetName = AccessMethodUtils.getStringConstant(funcArgs.get(2));
        retainInput = AccessMethodUtils.getBooleanConstant(funcArgs.get(3));
        requiresBroadcast = AccessMethodUtils.getBooleanConstant(funcArgs.get(4));
    }
    
    public String getIndexName() {
        return indexName;
    }

    public String getIndexType() {
        return indexType;
    }

    public String getDatasetName() {
        return datasetName;
    }

    public boolean getRetainInput() {
        return retainInput;
    }

    public boolean getRequiresBroadcast() {
        return requiresBroadcast;
    }
}

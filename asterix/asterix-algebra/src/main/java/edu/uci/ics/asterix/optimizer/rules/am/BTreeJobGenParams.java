package edu.uci.ics.asterix.optimizer.rules.am;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import edu.uci.ics.asterix.om.base.AInt32;
import edu.uci.ics.asterix.om.constants.AsterixConstantValue;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;

public class BTreeJobGenParams extends AccessMethodJobGenParams {

    protected List<LogicalVariable> lowKeyVarList;
    protected List<LogicalVariable> highKeyVarList;
    
    protected boolean lowKeyInclusive;
    protected boolean highKeyInclusive;
    
    public BTreeJobGenParams() {
        super();
    }
    
    public BTreeJobGenParams(String indexName, String indexType, String datasetName, boolean retainInput,
            boolean requiresBroadcast) {
        super(indexName, indexType, datasetName, retainInput, requiresBroadcast);
    }

    public void setLowKeyVarList(List<LogicalVariable> keyVarList, int startIndex, int numKeys) {
        lowKeyVarList = new ArrayList<LogicalVariable>(numKeys);
        setKeyVarList(keyVarList, lowKeyVarList, startIndex, numKeys);
    }
    
    public void setHighKeyVarList(List<LogicalVariable> keyVarList, int startIndex, int numKeys) {
        highKeyVarList = new ArrayList<LogicalVariable>(numKeys);
        setKeyVarList(keyVarList, highKeyVarList, startIndex, numKeys);
    }
    
    private void setKeyVarList(List<LogicalVariable> src, List<LogicalVariable> dest, int startIndex, int numKeys) {
        for (int i = 0; i < numKeys; i++) {
            dest.add(src.get(startIndex + i));
        }
    }
    
    public void setLowKeyInclusive(boolean lowKeyInclusive) {
        this.lowKeyInclusive = lowKeyInclusive;
    }
    
    public void setHighKeyInclusive(boolean highKeyInclusive) {
        this.highKeyInclusive = highKeyInclusive;
    }
    
    public void writeToFuncArgs(List<Mutable<ILogicalExpression>> funcArgs) {
        super.writeToFuncArgs(funcArgs);
        writeKeyVarList(lowKeyVarList, funcArgs);
        writeKeyVarList(highKeyVarList, funcArgs);
        writeKeyInclusive(lowKeyInclusive, funcArgs);
        writeKeyInclusive(highKeyInclusive, funcArgs);
    }
    
    public void readFromFuncArgs(List<Mutable<ILogicalExpression>> funcArgs) {
        super.readFromFuncArgs(funcArgs);
        int nextIndex = readKeyVarLists(funcArgs, 5);
        readKeyInclusives(funcArgs, nextIndex);
    }
    
    private int readKeyVarLists(List<Mutable<ILogicalExpression>> funcArgs, int index) {
        int numLowKeys = AccessMethodUtils.getInt32Constant(funcArgs.get(index));
        if (numLowKeys > 0) {
            lowKeyVarList = new ArrayList<LogicalVariable>(numLowKeys);
            for (int i = 0; i < numLowKeys; i++) {
                LogicalVariable var = ((VariableReferenceExpression) funcArgs.get(index + 1 + i).getValue())
                        .getVariableReference();
                lowKeyVarList.add(var);
            }
        }
        int highKeysIndex = index + numLowKeys + 1;
        int numHighKeys = AccessMethodUtils.getInt32Constant(funcArgs.get(index + numLowKeys + 1));
        if (numHighKeys > 0) {
            highKeyVarList = new ArrayList<LogicalVariable>(numHighKeys);
            for (int i = 0; i < numHighKeys; i++) {
                LogicalVariable var = ((VariableReferenceExpression) funcArgs.get(highKeysIndex + 1 + i).getValue())
                        .getVariableReference();
                highKeyVarList.add(var);
            }
        }
        return highKeysIndex + numHighKeys + 1;
    }
    
    private void readKeyInclusives(List<Mutable<ILogicalExpression>> funcArgs, int index) {
        lowKeyInclusive = ((ConstantExpression) funcArgs.get(index).getValue()).getValue().isTrue();
        highKeyInclusive = ((ConstantExpression) funcArgs.get(index).getValue()).getValue().isTrue();
    }
    
    private void writeKeyVarList(List<LogicalVariable> varList, List<Mutable<ILogicalExpression>> funcArgs) {
        // Write number of key vars.
        Mutable<ILogicalExpression> numKeysRef = new MutableObject<ILogicalExpression>(new ConstantExpression(
                new AsterixConstantValue(new AInt32(varList.size()))));
        funcArgs.add(numKeysRef);
        for (LogicalVariable keyVar : varList) {
            Mutable<ILogicalExpression> keyVarRef = new MutableObject<ILogicalExpression>(
                    new VariableReferenceExpression(keyVar));
            funcArgs.add(keyVarRef);
        }
    }
    
    private void writeKeyInclusive(boolean keyInclusive, List<Mutable<ILogicalExpression>> funcArgs) {
        ILogicalExpression keyExpr = keyInclusive ? ConstantExpression.TRUE : ConstantExpression.FALSE;
        funcArgs.add(new MutableObject<ILogicalExpression>(keyExpr));
    }
    
    public List<LogicalVariable> getLowKeyVarList() {
        return lowKeyVarList;
    }

    public List<LogicalVariable> getHighKeyVarList() {
        return highKeyVarList;
    }

    public boolean isLowKeyInclusive() {
        return lowKeyInclusive;
    }

    public boolean isHighKeyInclusive() {
        return highKeyInclusive;
    }
}

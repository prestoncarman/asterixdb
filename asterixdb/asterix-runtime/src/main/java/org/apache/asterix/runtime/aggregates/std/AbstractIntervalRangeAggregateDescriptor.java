package org.apache.asterix.runtime.aggregates.std;

import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.runningaggregates.base.AbstractRunningAggregateFunctionDynamicDescriptor;

public abstract class AbstractIntervalRangeAggregateDescriptor
        extends AbstractRunningAggregateFunctionDynamicDescriptor {
    private static final long serialVersionUID = 1L;
    IAType aggFieldType;

    @Override
    public void setImmutableStates(Object... types) {
        aggFieldType = (IAType) types[0];
    }
}

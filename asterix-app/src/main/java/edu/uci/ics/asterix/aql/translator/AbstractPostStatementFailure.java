package edu.uci.ics.asterix.aql.translator;

public abstract class AbstractPostStatementFailure implements IPostStatementFailure {

    private final Object[] object;

    public AbstractPostStatementFailure(Object[] object) {
        this.object = object;
    }

    public Object getObject(int index) {
        return object[index];
    }

}

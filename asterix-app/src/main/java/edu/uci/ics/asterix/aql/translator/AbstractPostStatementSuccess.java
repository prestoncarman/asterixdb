package edu.uci.ics.asterix.aql.translator;


public abstract class AbstractPostStatementSuccess implements IPostStatementSuccess {

    private final Object[] object;

    public AbstractPostStatementSuccess(Object[] object) {
        this.object = object;
    }

    public Object getObject(int index) {
        return object[index];
    }

}

package edu.uci.ics.asterix.aql.expression;

import edu.uci.ics.asterix.aql.base.Statement;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlExpressionVisitor;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlVisitorWithVoidReturn;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.om.functions.AsterixFunction;

public class FunctionDropStatement implements Statement {

    private AsterixFunction asterixFunction;
    private boolean ifExists;

    public FunctionDropStatement(AsterixFunction asterixFunction, boolean ifExists) {
        this.asterixFunction = asterixFunction;
        this.ifExists = ifExists;
    }

    @Override
    public Kind getKind() {
        return Kind.FUNCTION_DROP;
    }

    public AsterixFunction getAsterixFunction() {
        return asterixFunction;
    }

    public boolean getIfExists() {
        return ifExists;
    }

    @Override
    public <R, T> R accept(IAqlExpressionVisitor<R, T> visitor, T arg) throws AsterixException {
        return visitor.visitFunctionDropStatement(this, arg);
    }

    @Override
    public <T> void accept(IAqlVisitorWithVoidReturn<T> visitor, T arg) throws AsterixException {
        visitor.visit(this, arg);
    }

}

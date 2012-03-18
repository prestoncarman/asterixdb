package edu.uci.ics.asterix.aql.expression;

import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.asterix.aql.base.Statement;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlExpressionVisitor;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlVisitorWithVoidReturn;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.om.functions.AsterixFunction;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;

public class CreateFunctionStatement implements Statement {

    private AsterixFunction asterixFunction;
    private String functionBody;
    private boolean ifNotExists;
    private List<String> paramList;
    private final String dependencies;
    private final String returnType;
    private final String language;
    private final String functionKind;

    public AsterixFunction getAsterixFunction() {
        return asterixFunction;
    }

    public void setFunctionIdentifier(AsterixFunction asterixFunction) {
        this.asterixFunction = asterixFunction;
    }

    public String getFunctionBody() {
        return functionBody;
    }

    public void setFunctionBody(String functionBody) {
        this.functionBody = functionBody;
    }

    public void setIfNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    public CreateFunctionStatement(AsterixFunction asterixFunction, List<String> parameterList, String functionBody,
            String dependencies, String returnType, String language, String functionKind, boolean ifNotExists) {

        this.asterixFunction = asterixFunction;
        this.functionBody = functionBody;
        this.ifNotExists = ifNotExists;
        this.paramList = new ArrayList<String>();
        for (String var : parameterList) {
            this.paramList.add(var);
        }
        this.dependencies = dependencies;
        this.returnType = returnType;
        this.language = language;
        this.functionKind = functionKind;
    }

    public boolean getIfNotExists() {
        return this.ifNotExists;
    }

    @Override
    public Kind getKind() {
        return Kind.CREATE_FUNCTION;
    }

    public List<String> getParamList() {
        return paramList;
    }

    public String getDependencies() {
        return dependencies;
    }

    public String getReturnType() {
        return returnType;
    }

    public String getLanguage() {
        return language;
    }

    public void setParamList(List<String> paramList) {
        this.paramList = paramList;
    }

    public String getFunctionKind() {
        return functionKind;
    }

    @Override
    public <R, T> R accept(IAqlExpressionVisitor<R, T> visitor, T arg) throws AsterixException {
        return visitor.visit(this, arg);
    }

    @Override
    public <T> void accept(IAqlVisitorWithVoidReturn<T> visitor, T arg) throws AsterixException {
        visitor.visit(this, arg);
    }

}

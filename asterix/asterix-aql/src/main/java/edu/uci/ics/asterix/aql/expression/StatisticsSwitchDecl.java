/*
 * Copyright 2009-2010 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.aql.expression;

import edu.uci.ics.asterix.aql.base.Statement;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlExpressionVisitor;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlVisitorWithVoidReturn;
import edu.uci.ics.asterix.common.exceptions.AsterixException;

/**
 * @author rico
 * 
 */
public class StatisticsSwitchDecl implements Statement {

    private boolean statsEnabled;

    public StatisticsSwitchDecl(boolean statsSwitch) {
        this.statsEnabled = statsSwitch;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * edu.uci.ics.asterix.aql.base.IAqlExpression#accept(edu.uci.ics.asterix
     * .aql.expression.visitor.IAqlExpressionVisitor, java.lang.Object)
     */
    @Override
    public <R, T> R accept(IAqlExpressionVisitor<R, T> visitor, T arg) throws AsterixException {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * edu.uci.ics.asterix.aql.base.IAqlExpression#accept(edu.uci.ics.asterix
     * .aql.expression.visitor.IAqlVisitorWithVoidReturn, java.lang.Object)
     */
    @Override
    public <T> void accept(IAqlVisitorWithVoidReturn<T> visitor, T arg) throws AsterixException {
        visitor.visit(this, arg);
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.uci.ics.asterix.aql.base.Statement#getKind()
     */
    @Override
    public Kind getKind() {
        // TODO Auto-generated method stub
        return Kind.STATS_SWITCH;
    }

    public boolean isStatsEnabled() {
        return statsEnabled;
    }

    public void setStatsEnabled(boolean statsEnabled) {
        this.statsEnabled = statsEnabled;
    }

}

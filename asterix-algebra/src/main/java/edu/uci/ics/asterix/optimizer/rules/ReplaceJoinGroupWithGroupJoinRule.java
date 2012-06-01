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
package edu.uci.ics.asterix.optimizer.rules;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;

import org.apache.commons.lang3.mutable.Mutable;

import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.GroupJoinOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import edu.uci.ics.hyracks.algebricks.rewriter.util.GroupJoinUtils;

public class ReplaceJoinGroupWithGroupJoinRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {

    	// Check if first op is GROUP
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.GROUP) {
            return false;
        }
        
    	// Check if child op is a JOIN
        GroupByOperator gby = (GroupByOperator) op;
        Mutable<ILogicalOperator> opRef2 = op.getInputs().get(0);
        AbstractLogicalOperator son = (AbstractLogicalOperator) opRef2.getValue();
        if (son.getOperatorTag() != LogicalOperatorTag.INNERJOIN
                && son.getOperatorTag() != LogicalOperatorTag.LEFTOUTERJOIN) {
            return false;
        }
        
        AbstractBinaryJoinOperator join = (AbstractBinaryJoinOperator) son;
        Mutable<ILogicalOperator> joinBranchLeftRef = join.getInputs().get(0);
        Mutable<ILogicalOperator> joinBranchRightRef = join.getInputs().get(1);
        Collection<LogicalVariable> joinBranchLeftVars = new HashSet<LogicalVariable>();
        Collection<LogicalVariable> joinBranchRightVars = new HashSet<LogicalVariable>();
        //VariableUtilities.getLiveVariables(joinBranchRightRef.getValue(), joinBranchRightVars);

    	// Check if join condition is EQ
        if(!GroupJoinUtils.isHashJoinCondition(join.getCondition().getValue(), joinBranchLeftRef.getValue().getSchema(), joinBranchRightRef.getValue().getSchema(), joinBranchLeftVars, joinBranchRightVars))
        	return false;
        
        // check: 1) PK(or FD on PK) used in join; 2) if Group-By vars are in join expression
        Collection<LogicalVariable> joinVars = new HashSet<LogicalVariable>();
        LinkedList<LogicalVariable> groupVars = (LinkedList<LogicalVariable>) gby.getGbyVarList();
        join.getCondition().getValue().getUsedVariables(joinVars);
        
        joinVars.removeAll(joinBranchRightVars);
         // 1. PK
        boolean pkExists = false;
        for (LogicalVariable v : joinVars) {
        	if(context.findPrimaryKey(v) != null) {
        		pkExists = true;
        		break;
        	}
        }
        if (!pkExists)
        	return false;
    	
    	 // 2. Group-By vars
        for (LogicalVariable v : groupVars)
        	if (!joinVars.remove(v))
        		return false;
        if (!joinVars.isEmpty())
        	return false;
        
        // check if Aggregate vars are in join right branch
    	Collection<LogicalVariable> aggrVariables = new HashSet<LogicalVariable>();
        for (ILogicalPlan p : gby.getNestedPlans()){
            for (Mutable<ILogicalOperator> r : p.getRoots()) {
                AbstractLogicalOperator op1Abs = (AbstractLogicalOperator) r.getValue();
                if (op1Abs.getOperatorTag() == LogicalOperatorTag.AGGREGATE) {
                	AggregateOperator op1 = (AggregateOperator) op1Abs;
                	//Collection<LogicalVariable> usedVariables = new HashSet<LogicalVariable>();
                	VariableUtilities.getUsedVariables(op1, aggrVariables);
                }
            }
        }
    	if (!joinBranchRightVars.containsAll(aggrVariables))
    		return false;
        
        // create GroupJoin op and replace in plan
        // make same as merge of Join and Group constructors
        
        GroupByOperator gbyClone = new GroupByOperator(gby.getGroupByList(), gby.getDecorList(), gby.getNestedPlans());
        gbyClone.getInputs().clear();
        
        GroupJoinOperator groupJoinOp = new GroupJoinOperator(join.getJoinKind(), join.getCondition(), joinBranchLeftRef, joinBranchRightRef, gbyClone);
        opRef.setValue(groupJoinOp);

        return true;
    }
}
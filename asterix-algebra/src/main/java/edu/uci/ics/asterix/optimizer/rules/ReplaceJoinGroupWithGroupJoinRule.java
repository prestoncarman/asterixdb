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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.FileNotFoundException;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import edu.uci.ics.asterix.common.config.GlobalConfig;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalPlan;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractLogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ILogicalExpressionJobGen;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions.ComparisonKind;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.GroupByOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.GroupJoinOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.NestedTupleSourceOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import edu.uci.ics.hyracks.algebricks.core.algebra.plan.ALogicalPlanImpl;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.FunctionalDependency;
import edu.uci.ics.hyracks.algebricks.core.algebra.util.OperatorPropertiesUtil;
import edu.uci.ics.hyracks.algebricks.core.config.AlgebricksConfig;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import edu.uci.ics.hyracks.algebricks.rewriter.util.GroupJoinUtils;
import edu.uci.ics.hyracks.algebricks.runtime.base.IAggregateFunctionFactory;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;

public class ReplaceJoinGroupWithGroupJoinRule implements IAlgebraicRewriteRule {
//    	Logger LOGGER = AlgebricksConfig.ALGEBRICKS_LOGGER;
	Logger LOGGER = GlobalConfig.ASTERIX_LOGGER;

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context) {
        return false;
    }

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
    	try{
    	// Check if first op is GROUP
        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        LOGGER.finest("GROUPJOIN_REPLACE: " + op.getOperatorTag().toString() + " " + op.hashCode());
        if (op.getOperatorTag() != LogicalOperatorTag.GROUP) {
        	LOGGER.finest("GROUPJOIN_REPLACE: " + "Not Group");
            return false;
        }
        
        // Check if Group-By contains Aggregate Op
        GroupByOperator gby = (GroupByOperator) op;
        for(ILogicalPlan p : gby.getNestedPlans()){
        	for(Mutable<ILogicalOperator> o : p.getRoots()){
        		AbstractLogicalOperator nestedOp = (AbstractLogicalOperator) o.getValue();
        		boolean isOpAggOrNTS = false;
        		if(nestedOp.getOperatorTag() == LogicalOperatorTag.AGGREGATE){
        			AggregateOperator aggOp = (AggregateOperator)nestedOp;
        	        if (aggOp.getVariables().size() < 1) {
        	        	LOGGER.finest("GROUPJOIN_REPLACE: " + "No Agg variables!");
        	            return false;
        	        }
    	            List<Mutable<ILogicalExpression>> expressions = aggOp.getExpressions();
    	            for (int i = 0; i < expressions.size(); i++) {
    	                AggregateFunctionCallExpression aggFun = (AggregateFunctionCallExpression) expressions.get(i).getValue();
    	                if (aggFun.isTwoStep()){
    	                	LOGGER.finest("GROUPJOIN_REPLACE: " + "TwoStep Agg");
    	                	//return false;
    	                }
        	        }
        			isOpAggOrNTS = true;
        		}
        		if(nestedOp.getOperatorTag() == LogicalOperatorTag.NESTEDTUPLESOURCE || nestedOp.getOperatorTag() == LogicalOperatorTag.ASSIGN)
        			isOpAggOrNTS = true;
        		
        		if(!isOpAggOrNTS)
        			return false;
        	}
        	
        }
        
    	// Check if child op is a JOIN
        Mutable<ILogicalOperator> opRef2 = op.getInputs().get(0);
        AbstractLogicalOperator childOfOp = (AbstractLogicalOperator) opRef2.getValue();
        LOGGER.finest("GROUPJOIN_REPLACE: " + childOfOp.getOperatorTag().toString() + " " + childOfOp.hashCode());
    	
    	if (childOfOp.getOperatorTag() == LogicalOperatorTag.ASSIGN){
            opRef2 = childOfOp.getInputs().get(0);
            childOfOp = (AbstractLogicalOperator) opRef2.getValue();
            LOGGER.finest("GROUPJOIN_REPLACE: " + childOfOp.getOperatorTag().toString() + " " + childOfOp.hashCode());
    	}
    		
        if (childOfOp.getOperatorTag() != LogicalOperatorTag.INNERJOIN
                && childOfOp.getOperatorTag() != LogicalOperatorTag.LEFTOUTERJOIN) {
        	LOGGER.finest("GROUPJOIN_REPLACE: " + "Not Join");
            return false;
        }
        
        
        AbstractBinaryJoinOperator join = (AbstractBinaryJoinOperator) opRef2.getValue();
        Mutable<ILogicalOperator> joinBranchLeftRef = join.getInputs().get(0);
        Mutable<ILogicalOperator> joinBranchRightRef = join.getInputs().get(1);
        Collection<LogicalVariable> joinBranchLeftVars = new HashSet<LogicalVariable>();
        Collection<LogicalVariable> joinBranchRightVars = new HashSet<LogicalVariable>();
        List<ILogicalExpression> exprs = new LinkedList<ILogicalExpression>();
        exprs.add(join.getCondition().getValue());
        VariableUtilities.getLiveVariables(joinBranchRightRef.getValue(), joinBranchRightVars);

    	// Check if join condition is EQ
        ILogicalExpression e;
        AbstractFunctionCallExpression fexp;
        FunctionIdentifier fi;
        ComparisonKind ck;
        while(!exprs.isEmpty()) {
	        e = exprs.get(0);
        	switch (e.getExpressionTag()) {
	        case FUNCTION_CALL: {
	            fexp = (AbstractFunctionCallExpression) e;
	            fi = fexp.getFunctionIdentifier();
	        	
	            if (fi.equals(AlgebricksBuiltinFunctions.AND)) {
	                for (Mutable<ILogicalExpression> a : fexp.getArguments()) {
	                	exprs.add(a.getValue());
	                }
	            }
	            else {
	                ck = AlgebricksBuiltinFunctions.getComparisonType(fi);
	                if (ck != ComparisonKind.EQ) {
	                	LOGGER.finest("GROUPJOIN_REPLACE: " + "Not HashJoin1");
	                    return false;
	                }
	                ILogicalExpression opLeft = fexp.getArguments().get(0).getValue();
	                ILogicalExpression opRight = fexp.getArguments().get(1).getValue();
	                if (opLeft.getExpressionTag() != LogicalExpressionTag.VARIABLE
	                        || opRight.getExpressionTag() != LogicalExpressionTag.VARIABLE) {
	                	LOGGER.finest("GROUPJOIN_REPLACE: " + "Not HashJoin2");
	                    return false;
	                }
/*	                LogicalVariable var1 = ((VariableReferenceExpression) opLeft).getVariableReference();
	                if(inLeftAll == null)
	                	throw new AlgebricksException("inLeftAll is null");
	                if (inLeftAll.contains(var1)) {
	                    if (!outLeftFields.contains(var1))
	                    	outLeftFields.add(var1);
	                } else if (inRightAll.contains(var1) && !outRightFields.contains(var1)) {
	                    outRightFields.add(var1);
	                } else {
	                    return false;
	                }
	                LogicalVariable var2 = ((VariableReferenceExpression) opRight).getVariableReference();
	                if (inLeftAll.contains(var2) && !outLeftFields.contains(var2)) {
	                    outLeftFields.add(var2);
	                } else if (inRightAll.contains(var2) && !outRightFields.contains(var2)) {
	                    outRightFields.add(var2);
	                } else {
	                    return false;
	                }
	                return true;
*/
	                }
            	exprs.remove(0);
                break;
	            }
	        default: {
	        	LOGGER.finest("GROUPJOIN_REPLACE: " + "Not HashJoin3");
	        	return false;
	        	}
	        }
        }

        // check: 1) PK(or FD on PK) used in join; 2) if Group-By vars are in join expression
        Collection<LogicalVariable> joinVars = new HashSet<LogicalVariable>();
        List<LogicalVariable> groupVars = gby.getGbyVarList();
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
        if (!pkExists){
        	LOGGER.finest("GROUPJOIN_REPLACE: " + "No PK");
//        	return false;
        }
  	
    	 // 2. Group-By vars
        // check groupVars.size = joinVars.size
        if (groupVars.size() != joinVars.size()){
        	LOGGER.finest("GROUPJOIN_REPLACE: " + "Not |GroupVars| == |JoinVars| 1");
        	return false;
        }
        //List
        List<FunctionalDependency> gByFDList = null;
        for (LogicalVariable v : groupVars){
        	if (!joinVars.remove(v)){
        		LOGGER.finest("GROUPJOIN_REPLACE: " + "Not |GroupVars| == |JoinVars| 2");
        		LOGGER.finest("GROUPJOIN_REPLACE: " + v.getId());
            	if(context.findPrimaryKey(v) == null)
            		return false;
        	}
        }
        if (!joinVars.isEmpty()){
        	LOGGER.finest("GROUPJOIN_REPLACE: " + "Not |GroupVars| == |JoinVars| 3");
        	return false;
        }
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
	LOGGER.finest("GROUPJOIN_REPLACE: aggrVariables.size = " + aggrVariables.size());
    	if (!joinBranchRightVars.containsAll(aggrVariables)){
    		LOGGER.finest("GROUPJOIN_REPLACE: " + "Not AggrVar in Right branch");
    		return false;
    	}
        // create GroupJoin op and replace in plan
        // make same as merge of Join and Group constructors
        
        List<ILogicalPlan> nestedPlans = gby.getNestedPlans();
        context.computeAndSetTypeEnvironmentForOperator(joinBranchLeftRef.getValue());
        context.computeAndSetTypeEnvironmentForOperator(joinBranchRightRef.getValue());
        GroupJoinOperator groupJoinOp = new GroupJoinOperator(join.getJoinKind(), join.getCondition(), joinBranchLeftRef, joinBranchRightRef,
        		gby.getGroupByList(), gby.getDecorList(), nestedPlans);
        boolean foundNTS = true;
        for(ILogicalPlan p : nestedPlans){
        	Mutable<ILogicalOperator> oRef = p.getRoots().get(0);
        	AbstractLogicalOperator absOp = (AbstractLogicalOperator)oRef.getValue();
        	foundNTS &= updateNTSInNestedPlan(absOp, groupJoinOp, context);
/*        	if(absOp.getOperatorTag() != LogicalOperatorTag.NESTEDTUPLESOURCE){
        		absOp = (AbstractLogicalOperator)absOp.getInputs().get(0).getValue();
        	}
        	else{
                NestedTupleSourceOperator nts = (NestedTupleSourceOperator)absOp;
                nts.getDataSourceReference().setValue(groupJoinOp);
                context.computeAndSetTypeEnvironmentForOperator(nts);
                foundNTS = true;
                AggregateOperator agg = (AggregateOperator)absOp;
                List<Mutable<ILogicalOperator>> aggInpList = agg.getInputs();
                aggInpList.clear();
                aggInpList.add(new MutableObject<ILogicalOperator>(nts));
                ILogicalPlan np1 = new ALogicalPlanImpl(oRef);
                nestedPlans.add(np1);
                context.computeAndSetTypeEnvironmentForOperator(agg);
                OperatorPropertiesUtil.typeOpRec(oRef, context);
        	}
*/        }
        if(!foundNTS)
        	return false;
       
        groupJoinOp.setNestedPlans(nestedPlans);
        
        // context.addToDontApplySet(new IntroduceCombinerRule(), groupJoinOp);
        context.computeAndSetTypeEnvironmentForOperator(groupJoinOp);
        
        LOGGER.finest("GROUPJOIN_REPLACE: GroupJoin nestedPlans size = " + groupJoinOp.getNestedPlans().size());
        
        for (ILogicalPlan p : groupJoinOp.getNestedPlans()){
            LOGGER.finest("GROUPJOIN_REPLACE: GroupJoin nestedPlan root size = " + p.getRoots().size());
            for (Mutable<ILogicalOperator> r : p.getRoots()) {
                AbstractLogicalOperator op1Abs = (AbstractLogicalOperator) r.getValue();
                if (op1Abs != null) {
                    LOGGER.finest("GROUPJOIN_REPLACE: GroupJoin nestedOp = " + op1Abs.getOperatorTag().name());
                	Collection<LogicalVariable> vars = new HashSet<LogicalVariable>();
                	VariableUtilities.getLiveVariables(op1Abs, vars);
                	for(LogicalVariable v : vars){
                        LOGGER.finest("\t** GROUPJOIN_REPLACE: var = " + v.getId());
                	}
                	for(Mutable<ILogicalOperator> mo: op1Abs.getInputs()){
                        AbstractLogicalOperator childAbs = (AbstractLogicalOperator) mo.getValue();
                        LOGGER.finest("\t** GROUPJOIN_REPLACE: childOp = " + childAbs.getOperatorTag().name());
                    	Collection<LogicalVariable> childVars = new HashSet<LogicalVariable>();
                    	VariableUtilities.getLiveVariables(childAbs, childVars);
                    	for(LogicalVariable v : childVars){
                            LOGGER.finest("\t\t** GROUPJOIN_REPLACE: var = " + v.getId());
                    	}
                	}
                }
            }
        }

        opRef.setValue(groupJoinOp);

        LOGGER.finest("GROUPJOIN_REPLACE: " + "GroupJoin match");
        return true;
    	} catch (Exception e){
    		LOGGER.finest("GROUPJOIN_REPLACE: Error = " + e.getMessage());
    		return false;
    	}
    }
    
    private boolean updateNTSInNestedPlan(AbstractLogicalOperator op,
    		GroupJoinOperator gjOp, IOptimizationContext context) throws AlgebricksException{
    	LOGGER.finest("GROUPJOIN_REPLACE: " + "NestedPlan \t" + op.getOperatorTag().toString());
    	if(op.getOperatorTag() != LogicalOperatorTag.NESTEDTUPLESOURCE){
    		boolean foundNTS = updateNTSInNestedPlan((AbstractLogicalOperator)op.getInputs().get(0).getValue(), gjOp, context);
            context.computeAndSetTypeEnvironmentForOperator(op);
            return foundNTS;
    	}
    	else{
            NestedTupleSourceOperator nts = (NestedTupleSourceOperator)op;
            nts.getDataSourceReference().setValue(gjOp);
            context.computeAndSetTypeEnvironmentForOperator(nts);
            return true;
        	}
    }
}


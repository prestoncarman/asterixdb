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
package edu.uci.ics.asterix.algebra.operators.physical;

import edu.uci.ics.asterix.jobgen.StatisticsRuntimeFactory;
import edu.uci.ics.asterix.metadata.declared.AqlSourceId;
import edu.uci.ics.asterix.metadata.statistics.BaseStatistics;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IHyracksJobBuilder;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.PhysicalOperatorTag;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.IOperatorSchema;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.StatisticsOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.physical.AbstractPhysicalOperator;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.IPhysicalPropertiesVector;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.PhysicalRequirements;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.jobgen.impl.JobGenContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.jobgen.impl.JobGenHelper;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;

/**
 * @author rico
 * 
 */
public class StatisticsPOperator extends AbstractPhysicalOperator {

    private BaseStatistics statsData;

    @Override
    public PhysicalOperatorTag getOperatorTag() {
        return PhysicalOperatorTag.STATS;
    }

    @Override
    public PhysicalRequirements getRequiredPropertiesForChildren(ILogicalOperator op,
            IPhysicalPropertiesVector reqdByParent) {
        return emptyUnaryRequirements();
    }

    @Override
    public void computeDeliveredProperties(ILogicalOperator op, IOptimizationContext context)
            throws AlgebricksException {
        ILogicalOperator op2 = op.getInputs().get(0).getValue();
        deliveredProperties = op2.getDeliveredPhysicalProperties().clone();
    }

    @Override
    public void contributeRuntimeOperator(IHyracksJobBuilder builder, JobGenContext context, ILogicalOperator op,
            IOperatorSchema propagatedSchema, IOperatorSchema[] inputSchemas, IOperatorSchema outerPlanSchema)
            throws AlgebricksException {
        StatisticsOperator statsOp = (StatisticsOperator) op;

        RecordDescriptor recDesc = JobGenHelper.mkRecordDescriptor(op, propagatedSchema, context);
        StatisticsRuntimeFactory runtime = new StatisticsRuntimeFactory(null, this.statsData, null);
        builder.contributeMicroOperator(statsOp, runtime, recDesc);
        // and contribute one edge from its child
        ILogicalOperator src = statsOp.getInputs().get(0).getValue();
        builder.contributeGraphEdge(src, 0, statsOp, 0);
    }

    @Override
    public boolean isMicroOperator() {
        return true;
    }

    public void createStatsDataObject(AqlSourceId aqlSourceId) {
        this.statsData = new BaseStatistics(aqlSourceId);
    }

}

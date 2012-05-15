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
package edu.uci.ics.asterix.optimizer.base;

import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IExpressionEvalSizeComputer;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IExpressionTypeComputer;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IMergeAggregationExpressionFactory;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.INullableTypeComputer;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.AlgebricksOptimizationContext;
import edu.uci.ics.hyracks.algebricks.core.rewriter.base.PhysicalOptimizationConfig;

/**
 * @author rico
 * 
 */
public class AsterixOptimizationContext extends AlgebricksOptimizationContext {

    private boolean statisticsEnabled = false;

    /**
     * @param varCounter
     * @param frameSize
     * @param expressionEvalSizeComputer
     * @param mergeAggregationExpressionFactory
     * @param expressionTypeComputer
     * @param nullableTypeComputer
     * @param physicalOptimizationConfig
     * @param statisticsEnabled
     */
    public AsterixOptimizationContext(int varCounter, int frameSize,
            IExpressionEvalSizeComputer expressionEvalSizeComputer,
            IMergeAggregationExpressionFactory mergeAggregationExpressionFactory,
            IExpressionTypeComputer expressionTypeComputer, INullableTypeComputer nullableTypeComputer,
            PhysicalOptimizationConfig physicalOptimizationConfig) {
        super(varCounter, frameSize, expressionEvalSizeComputer, mergeAggregationExpressionFactory,
                expressionTypeComputer, nullableTypeComputer, physicalOptimizationConfig);
        // TODO Auto-generated constructor stub
    }

    public boolean isStatisticsEnabled() {
        return statisticsEnabled;
    }

    public void setStatisticsEnabled(boolean statisticsEnabled) {
        this.statisticsEnabled = statisticsEnabled;
    }

}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.runtime.evaluators.common;

import org.apache.asterix.fuzzyjoin.similarity.SimilarityMetricJaccard;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class SimilarityJaccardSortedCheckEvaluator extends SimilarityJaccardCheckEvaluator {

    protected final SimilarityMetricJaccard jaccard = new SimilarityMetricJaccard();

    public SimilarityJaccardSortedCheckEvaluator(IScalarEvaluatorFactory[] args, IHyracksTaskContext context)
            throws AlgebricksException {
        super(args, context);
    }

    @Override
    protected float computeResult() throws AlgebricksException {
        try {
            return jaccard.getSimilarity(firstListIter, secondListIter, jaccThresh);
        } catch (HyracksDataException e) {
            throw new AlgebricksException(e);
        }
    }
}
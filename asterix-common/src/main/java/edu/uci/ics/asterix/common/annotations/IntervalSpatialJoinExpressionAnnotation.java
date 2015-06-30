/*
 * Copyright 2009-2013 by The Regents of the University of California
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
package edu.uci.ics.asterix.common.annotations;

import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IExpressionAnnotation;

public class IntervalSpatialJoinExpressionAnnotation implements IExpressionAnnotation {

    public static final String HINT_STRING = "interval-spatial-join";
    public static final IntervalSpatialJoinExpressionAnnotation INSTANCE = new IntervalSpatialJoinExpressionAnnotation();

    private Object object;

    @Override
    public Object getObject() {
        return object;
    }

    @Override
    public void setObject(Object object) {
        this.object = object;
    }

    @Override
    public IExpressionAnnotation copy() {
        IntervalSpatialJoinExpressionAnnotation clone = new IntervalSpatialJoinExpressionAnnotation();
        clone.setObject(object);
        return clone;
    }
}

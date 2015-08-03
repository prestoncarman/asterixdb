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
package edu.uci.ics.asterix.dataflow.data.nontagged.comparators.allenrelations;

import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AIntervalSerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;

public class OverlapIntervalBinaryComparatorFactory implements IBinaryComparatorFactory {

    private static final long serialVersionUID = 1L;

    public static final OverlapIntervalBinaryComparatorFactory INSTANCE = new OverlapIntervalBinaryComparatorFactory();

    private OverlapIntervalBinaryComparatorFactory() {

    }

    @Override
    public IBinaryComparator createBinaryComparator() {
        return new IBinaryComparator() {

            @Override
            public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
                long start0 = AIntervalSerializerDeserializer.getIntervalStart(b1, s1);
                long end0 = AIntervalSerializerDeserializer.getIntervalEnd(b1, s1);
                long start1 = AIntervalSerializerDeserializer.getIntervalStart(b2, s2);
                long end1 = AIntervalSerializerDeserializer.getIntervalEnd(b2, s2);

                if (start0 < start1 && end0 > start1 && end1 > end0) {
                    // These intervals overlap
                    return 0;
                }
                if (end0 < start1) {
                    return 1;
                }
                return -1;
            }
        };
    }
}

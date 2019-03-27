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
package org.apache.hyracks.data.std.accessors;

import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryRangeComparatorFactory;
import org.apache.hyracks.data.std.api.IComparable;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.api.IPointableFactory;

public class PointableBinaryRangeDescComparatorFactory implements IBinaryRangeComparatorFactory {
    private static final long serialVersionUID = 1L;

    protected final IPointableFactory pf;

    public PointableBinaryRangeDescComparatorFactory(IPointableFactory pf) {
        this.pf = pf;
    }

    public static PointableBinaryRangeDescComparatorFactory of(IPointableFactory pf) {
        return new PointableBinaryRangeDescComparatorFactory(pf);
    }

    @Override
    public IBinaryComparator createMinBinaryComparator() {
        final IPointable p = pf.createPointable();
        return new IBinaryComparator() {
            @Override
            public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
                if (l1 == 0 && l2 != 0) {
                    return -1;
                }
                if (l1 != 0 && l2 == 0) {
                    return 1;
                }
                p.set(b1, s1, l1);
                return -((IComparable) p).compareTo(b2, s2, l2);
            }
        };
    }

    @Override
    public IBinaryComparator createMaxBinaryComparator() {
        return createMinBinaryComparator();
    }
}

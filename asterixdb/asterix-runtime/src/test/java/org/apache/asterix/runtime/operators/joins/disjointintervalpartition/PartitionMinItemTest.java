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
package org.apache.asterix.runtime.operators.joins.disjointintervalpartition;

import static org.junit.Assert.*;

import java.util.Comparator;
import java.util.PriorityQueue;

import org.apache.hyracks.dataflow.std.structures.TuplePointer;
import org.junit.Test;

public class PartitionMinItemTest {

    @Test
    public void testBasics() {
        PartitionMinItem pmi = new PartitionMinItem();
        assertEquals(-1, pmi.getPartition());
        assertEquals(Long.MIN_VALUE, pmi.getPoint());

        PartitionMinItem pmi2 = new PartitionMinItem(2, 222222222L);
        assertEquals(2, pmi2.getPartition());
        assertEquals(222222222L, pmi2.getPoint());

        pmi.reset(pmi2);
        assertEquals(2, pmi.getPartition());
        assertEquals(222222222L, pmi.getPoint());

        pmi.setPoint(3333333333L);
        assertEquals(2, pmi.getPartition());
        assertEquals(3333333333L, pmi.getPoint());
    }

    @Test
    public void testPointComparisons() {
        PartitionMinItem pmi1 = new PartitionMinItem(2, 2222222222L);
        PartitionMinItem pmi2 = new PartitionMinItem(2, 3333333333L);
        PartitionMinItem pmi3 = new PartitionMinItem(2, 3333333333L);

        assertTrue(pmi1.compareTo(pmi2) < 0);
        assertTrue(pmi1.compareTo(pmi2) <= 0);
        assertTrue(pmi2.compareTo(pmi3) == 0);
        assertTrue(pmi2.compareTo(pmi3) <= 0);
    }

    @Test
    public void testPriorityQueueWithNaturalOrdering() {
        PriorityQueue<PartitionMinItem> pq = new PriorityQueue<>(16);
        subtestPriorityQueue(pq);
    }

    public void subtestPriorityQueue(PriorityQueue<PartitionMinItem> pq) {
        pq.add(new PartitionMinItem(2, 2222222222L));
        assertEquals(1, pq.size());
        assertEquals(2222222222L, pq.peek().getPoint());

        pq.add(new PartitionMinItem(2, 3333333333L));
        assertEquals(2, pq.size());
        assertEquals(2222222222L, pq.peek().getPoint());

        PartitionMinItem pmi = pq.poll();
        assertEquals(1, pq.size());
        assertEquals(3333333333L, pq.peek().getPoint());
        assertEquals(2222222222L, pmi.getPoint());

        pq.add(new PartitionMinItem(2, 1111111111L));
        assertEquals(2, pq.size());
        assertEquals(1111111111L, pq.peek().getPoint());
    }

    
}
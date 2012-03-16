/*
 * Copyright 2011-2012 by The Regents of the University of California
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

package edu.uci.ics.asterix.transaction.management.service.transaction;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Any operation in this class should follow MGL(Multiple Granularity Locking) protocol.
 * Currently, this class provides the following lock granularity with read and write mode only(no intention mode).
 * Must follow Crabbing protocol: 
 * Writer should get write lock from the root resource, then the acquired write lock can be released after getting the child resource's write lock, and so on. 
 * Reader should get read lock as writer does.  
 * 
 * tableLatch -----> entryLatch ---> lsnLatch
 *                              |
 *                              ---> transactionIdLatch  
 */
public class MemoryComponentTable {
    private static HashMap<ByteBuffer, MemoryComponentTableEntry> memoryComponentTable = new HashMap<ByteBuffer, MemoryComponentTableEntry>();
    private static final ReadWriteLock tableLatch = new ReentrantReadWriteLock(true);

    public static long getMinFirstLSN() {
        long minFirstLSN = Long.MAX_VALUE;
        long curLSN;
        tableLatch.readLock().lock();
        Set<Map.Entry<ByteBuffer, MemoryComponentTableEntry>> entries = memoryComponentTable.entrySet();
        
        for (Map.Entry<ByteBuffer, MemoryComponentTableEntry> entry : entries) {
            curLSN = entry.getValue().firstLSN;
            if (minFirstLSN > curLSN) {
                minFirstLSN = curLSN;
            }
        }
        
        try {
            return minFirstLSN;
        } finally {
            tableLatch.readLock().unlock();
        }
    }
    
    //public void registerMemoryComponent(byte[] resourceId, )
    //EEEEEEEEEEEEEEEEEEEEEEEEeee

    public class MemoryComponentTableEntry {
        public long firstLSN;
        public long lastLSN;
        public Map transactionIds;
        public ReadWriteLock entryLatch;
        public ReadWriteLock lsnLatch;
        public ReadWriteLock transactionIdLatch;

        public MemoryComponentTableEntry() {
            firstLSN = -1;
            lastLSN = -1;
            transactionIds = new HashMap();
            entryLatch = new ReentrantReadWriteLock(true);
            lsnLatch = new ReentrantReadWriteLock(true);
            transactionIdLatch = new ReentrantReadWriteLock(true);
        }

        public long getFirstLSN() {
            entryLatch.readLock().lock();
            try {
                return firstLSN;
            } finally {
                entryLatch.readLock().unlock();
            }
        }

        public long getLastLSN() {
            entryLatch.readLock().lock();
            try {
                return lastLSN;
            } finally {
                entryLatch.readLock().unlock();
            }
        }

        public void setLastLSN(long lsn) {
            entryLatch.writeLock().lock();
            if (firstLSN == -1) {
                firstLSN = lsn;
            }
            lastLSN = lsn;
            entryLatch.writeLock().unlock();
        }
    }
}

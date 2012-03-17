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
 * Currently, this class provides the following lock granularity with read and write mode only(no intention mode).
 * tableLatch -----> entryLatch ---> lsnLatch
 *                            | 
 *                              ---> transactionIdLatch
 * TODO Currently, this code only use tableLatch. When higher concurrency than partition level,
 * finer granule latch should be used correctly.
 */
public class MemoryComponentTable {
    private HashMap<ByteBuffer, MemoryComponentTableEntry> memoryComponentTable = new HashMap<ByteBuffer, MemoryComponentTableEntry>();
    private final ReadWriteLock tableLatch = new ReentrantReadWriteLock(true);

    public long getMinFirstLSN() {
        long minFirstLSN = Long.MAX_VALUE;
        long curLSN;
        tableLatch.readLock().lock();
        Set<Map.Entry<ByteBuffer, MemoryComponentTableEntry>> entries = memoryComponentTable.entrySet();

        for (Map.Entry<ByteBuffer, MemoryComponentTableEntry> entry : entries) {
            curLSN = entry.getValue().getFirstLSN();
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

    /**
     * updates the existing resource's information. 
     * If the resource doesn't exist, it is created and then the information is inserted.
     * @param resourceBytes
     * @param lsn
     * @param txnId
     */
    public void upsertLSN(byte[] resourceBytes, long lsn, long txnId) {
        ByteBuffer resourceId = ByteBuffer.wrap(resourceBytes);

        //check whether the resource is existing.
        //If it doesn't exist, create an entry.
        tableLatch.writeLock().lock();
        MemoryComponentTableEntry mctEntry = memoryComponentTable.get(resourceId);
        if (mctEntry == null) {
            mctEntry = new MemoryComponentTableEntry();
            mctEntry.upsertTransactionId(txnId);
        }
        
        //update LSN
        mctEntry.setLastLSN(lsn);
        tableLatch.writeLock().unlock();
    }
    
    public void removeTransactionId(byte[] resourceBytes, long txnId) {
        ByteBuffer resourceId = ByteBuffer.wrap(resourceBytes);
        tableLatch.writeLock().lock();
        MemoryComponentTableEntry mctEntry = memoryComponentTable.get(resourceId);
        if (mctEntry != null) {
            mctEntry.deleteTransactionId(txnId);
        }
        tableLatch.writeLock().unlock();
    }

    /**
     * Deletes a requested resource. 
     * Regardless of the resource's existence, remove() is called.
     * @param resourceBytes
     */
    public void deleteResource(byte[] resourceBytes) {
        ByteBuffer resourceId = ByteBuffer.wrap(resourceBytes);
        
        tableLatch.writeLock().lock();
        memoryComponentTable.remove(resourceId);
        tableLatch.writeLock().unlock();
    }

    public class MemoryComponentTableEntry {
        private long firstLSN;
        private long lastLSN;
        private HashMap transactionIds;

        //        public ReadWriteLock entryLatch;
        //        public ReadWriteLock lsnLatch;
        //        public ReadWriteLock transactionIdLatch;

        public MemoryComponentTableEntry() {
            firstLSN = -1;
            lastLSN = -1;
            transactionIds = new HashMap();
            //            entryLatch = new ReentrantReadWriteLock(true);
            //            lsnLatch = new ReentrantReadWriteLock(true);
            //            transactionIdLatch = new ReentrantReadWriteLock(true);
        }

        public long getFirstLSN() {
            //            entryLatch.readLock().lock();
            //            try {
            return firstLSN;
            //            } finally {
            //                entryLatch.readLock().unlock();
            //            }
        }

        public long getLastLSN() {
            //            entryLatch.readLock().lock();
            //            try {
            return lastLSN;
            //            } finally {
            //                entryLatch.readLock().unlock();
            //            }
        }

        public void setLastLSN(long lsn) {
            //            entryLatch.writeLock().lock();
            if (firstLSN == -1) {
                firstLSN = lsn;
            }
            lastLSN = lsn;
            //            entryLatch.writeLock().unlock();
        }
        
        public void upsertTransactionId(long txnId) {
            transactionIds.put(txnId, 0);
        }
        
        public void deleteTransactionId(long txnId) {
            transactionIds.remove(txnId);
        }
    }
}

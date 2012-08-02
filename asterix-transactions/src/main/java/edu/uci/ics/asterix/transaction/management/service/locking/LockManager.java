/*
 * Copyright 2009-2012 by The Regents of the University of California
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

package edu.uci.ics.asterix.transaction.management.service.locking;

import java.io.Serializable;
import java.util.HashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.LogManager;
import java.util.logging.Logger;

import edu.uci.ics.asterix.transaction.management.exception.ACIDException;
import edu.uci.ics.asterix.transaction.management.service.logging.LogActionType;
import edu.uci.ics.asterix.transaction.management.service.logging.LogType;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionContext;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionManagementConstants.LockManagerConstants.LockMode;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionProvider;
import edu.uci.ics.hyracks.api.job.JobId;

/**
 * An implementation of the ILockManager interface for the
 * specific case of locking protocol with two lock modes: (S) and (X),
 * where S lock mode is shown by 0, and X lock mode is shown by 1.
 * 
 * @author pouria, kisskys
 */

public class LockManager implements ILockManager {

    private static final Logger LOGGER = Logger.getLogger(LockManager.class.getName());
    private static final int LOCK_MANAGER_INITIAL_HASH_TABLE_SIZE = 50;// do we need this?
    public static final boolean IS_DEBUG_MODE = false;

    private TransactionProvider txnProvider;

    //all threads accessing to LockManager's tables such as jobHT and datasetResourceHT
    //are serialized through LockTableLatch. All threads waiting the latch will be fairly served
    //in FIFO manner when the latch is available. 
    private final ReadWriteLock lockTableLatch;
    private final ReadWriteLock waiterLatch;
    private HashMap<JobId, JobInfo> jobHT;
    private HashMap<DatasetId, DatasetLockInfo> datasetResourceHT;

    private EntityLockInfoManager entityLockInfoManager;
    private EntityInfoManager entityInfoManager;
    private LockWaiterManager lockWaiterManager;

    private DeadlockDetector deadlockDetector;
    private TimeOutDetector toutDetector;

    public LockManager(TransactionProvider txnProvider) throws ACIDException {
        this.txnProvider = txnProvider;
        this.lockTableLatch = new ReentrantReadWriteLock(true);
        this.waiterLatch = new ReentrantReadWriteLock(true);
        this.jobHT = new HashMap<JobId, JobInfo>();
        this.datasetResourceHT = new HashMap<DatasetId, DatasetLockInfo>();
        this.entityInfoManager = new EntityInfoManager();
        this.entityLockInfoManager = new EntityLockInfoManager(entityInfoManager, lockWaiterManager);
        this.lockWaiterManager = new LockWaiterManager();
        this.deadlockDetector = new DeadlockDetector(jobHT, datasetResourceHT, entityLockInfoManager,
                entityInfoManager, lockWaiterManager);
        this.toutDetector = new TimeOutDetector(this);
    }

    @Override
    public void lock(DatasetId datasetId, int entityHashValue, byte lockMode, TransactionContext txnContext)
            throws ACIDException {
        JobId jobId = txnContext.getJobId();
        int jId = jobId.getId(); //int-type jobId
        int dId = datasetId.getId(); //int-type datasetId
        int entityInfo;
        int eLockInfo = -1;
        DatasetLockInfo dLockInfo;
        JobInfo jobInfo;
        byte datasetLockMode = entityHashValue == -1 ? lockMode : lockMode == LockMode.S ? LockMode.IS : LockMode.IX;

        latchLockTable();

        dLockInfo = datasetResourceHT.get(datasetId);
        jobInfo = jobHT.get(jobId);

        //#. if the datasetLockInfo doesn't exist in datasetResourceHT 
        if (dLockInfo == null || dLockInfo.isNoHolder()) {
            if (dLockInfo == null) {
                dLockInfo = new DatasetLockInfo(entityLockInfoManager, entityInfoManager, lockWaiterManager);
                datasetResourceHT.put(new DatasetId(dId), dLockInfo); //datsetId obj should be created
            }
            entityInfo = entityInfoManager.allocate(jId, dId, entityHashValue, lockMode);

            //if dataset-granule lock
            if (entityHashValue == -1) { //-1 stands for dataset-granule
                entityInfoManager.increaseDatasetLockCount(entityInfo);
                dLockInfo.increaseLockCount(datasetLockMode);
                dLockInfo.addHolder(entityInfo);
            } else {
                entityInfoManager.increaseDatasetLockCount(entityInfo);
                dLockInfo.increaseLockCount(datasetLockMode);
                //add entityLockInfo
                eLockInfo = entityLockInfoManager.allocate();
                dLockInfo.getEntityResourceHT().put(entityHashValue, eLockInfo);
                entityInfoManager.increaseEntityLockCount(entityInfo);
                entityLockInfoManager.increaseLockCount(eLockInfo, lockMode);
                entityLockInfoManager.addHolder(eLockInfo, entityInfo);
            }

            if (jobInfo == null) {
                jobInfo = new JobInfo(entityInfoManager, lockWaiterManager, txnContext);
                jobHT.put(jobId, jobInfo); //jobId obj doesn't have to be created
            }
            jobInfo.addHoldingResource(entityInfo);

            unlatchLockTable();
            return;
        }

        //#. the datasetLockInfo exists in datasetResourceHT.
        //1. handle dataset-granule lock
        entityInfo = lockDatasetGranule(datasetId, entityHashValue, lockMode, txnContext);

        //2. handle entity-granule lock
        if (entityHashValue != -1) {
            lockEntityGranule(datasetId, entityHashValue, lockMode, entityInfo, txnContext);
        }

        unlatchLockTable();
        return;
    }

    private int lockDatasetGranule(DatasetId datasetId, int entityHashValue, byte lockMode, TransactionContext txnContext)
            throws ACIDException {
        JobId jobId = txnContext.getJobId();
        int jId = jobId.getId(); //int-type jobId
        int dId = datasetId.getId(); //int-type datasetId
        int waiterObjId;
        int entityInfo = -1;
        DatasetLockInfo dLockInfo;
        JobInfo jobInfo;
        boolean isUpgrade = false;
        int weakerModeLockCount;
        int waiterCount = 0;
        byte datasetLockMode = entityHashValue == -1 ? lockMode : lockMode == LockMode.S ? LockMode.IS : LockMode.IX;

        dLockInfo = datasetResourceHT.get(datasetId);
        jobInfo = jobHT.get(jobId);

        //check duplicated call

        //1. lock request causing duplicated upgrading requests from different threads in a same job
        waiterObjId = dLockInfo.findUpgraderFromUpgraderList(jId, entityHashValue);
        if (waiterObjId != -1) {
            //make the caller wait on the same LockWaiter object
            entityInfo = lockWaiterManager.getLockWaiter(waiterObjId).getEntityInfoSlot();
            waiterCount = handleLockWaiter(dLockInfo, -1, entityInfo, true, true, txnContext, jobInfo, waiterObjId);

            //Only for the first-get-up thread, the waiterCount will be more than 0 and
            //the thread update lock count on behalf of the all other waiting threads.
            //Therefore, all the next-get-up threads will not update any lock count.
            if (waiterCount > 0) {
                //add ((the number of waiting upgrader) - 1) to entityInfo's dataset lock count and datasetLockInfo's lock count
                //where -1 is for not counting the first upgrader's request since the lock count for the first upgrader's request
                //is already counted.
                weakerModeLockCount = entityInfoManager.getDatasetLockCount(entityInfo);
                entityInfoManager.setDatasetLockMode(entityInfo, lockMode);
                entityInfoManager.increaseDatasetLockCount(entityInfo, waiterCount - 1);

                if (entityHashValue == -1) { //dataset-granule lock
                    dLockInfo.increaseLockCount(LockMode.X, weakerModeLockCount + waiterCount - 1);//new lock mode
                    dLockInfo.decreaseLockCount(LockMode.S, weakerModeLockCount);//current lock mode
                } else {
                    dLockInfo.increaseLockCount(LockMode.IX, weakerModeLockCount + waiterCount - 1);
                    dLockInfo.decreaseLockCount(LockMode.IS, weakerModeLockCount);
                }
            }

            return entityInfo;
        }

        //2. lock request causing duplicated waiting requests from different threads in a same job
        waiterObjId = dLockInfo.findWaiterFromWaiterList(jId, entityHashValue);
        if (waiterObjId != -1) {
            //make the caller wait on the same LockWaiter object
            entityInfo = lockWaiterManager.getLockWaiter(waiterObjId).getEntityInfoSlot();
            waiterCount = handleLockWaiter(dLockInfo, -1, entityInfo, false, true, txnContext, jobInfo, waiterObjId);

            if (waiterCount > 0) {
                entityInfoManager.increaseDatasetLockCount(entityInfo, waiterCount);
                if (entityHashValue == -1) {
                    dLockInfo.increaseLockCount(datasetLockMode, waiterCount);
                    dLockInfo.addHolder(entityInfo);
                } else {
                    dLockInfo.increaseLockCount(datasetLockMode, waiterCount);
                    //IS and IX holders are implicitly handled.
                }
                //add entityInfo to JobInfo's holding-resource list
                jobInfo.addHoldingResource(entityInfo);
            }

            return entityInfo;
        }

        //3. lock request causing duplicated holding requests from different threads or a single thread in a same job
        entityInfo = dLockInfo.findEntityInfoFromHolderList(jId, entityHashValue);
        if (entityInfo == -1) { //new call from this job -> doesn't mean that eLockInfo doesn't exist since another thread might have create the eLockInfo already.
            entityInfo = entityInfoManager.allocate(jId, dId, entityHashValue, lockMode);
            if (jobInfo == null) {
                jobInfo = new JobInfo(entityInfoManager, lockWaiterManager, txnContext);
                jobHT.put(jobId, jobInfo);
            }

            //wait if any upgrader exists or upgrading lock mode is not compatible
            if (dLockInfo.getFirstUpgrader() != -1 || !dLockInfo.isCompatible(datasetLockMode)) {
                waiterCount = handleLockWaiter(dLockInfo, -1, entityInfo, false, true, txnContext, jobInfo, -1);
            }

            if (waiterCount > 0) {
                entityInfoManager.increaseDatasetLockCount(entityInfo);
                if (entityHashValue == -1) {
                    dLockInfo.increaseLockCount(datasetLockMode);
                    dLockInfo.addHolder(entityInfo);
                } else {
                    dLockInfo.increaseLockCount(datasetLockMode);
                    //IS and IX holders are implicitly handled.
                }
                //add entityInfo to JobInfo's holding-resource list
                jobInfo.addHoldingResource(entityInfo);
            }
        } else {
            isUpgrade = isLockUpgrade(entityInfoManager.getDatasetLockMode(entityInfo), lockMode);
            if (isUpgrade) { //upgrade call 
                //wait if any upgrader exists or upgrading lock mode is not compatible
                if (dLockInfo.getFirstUpgrader() != -1 || !dLockInfo.isUpgradeCompatible(datasetLockMode, entityInfo)) {
                    waiterCount = handleLockWaiter(dLockInfo, -1, entityInfo, true, true, txnContext, jobInfo, -1);
                }

                if (waiterCount > 0) {
                    //add ((the number of waiting upgrader) - 1) to entityInfo's dataset lock count and datasetLockInfo's lock count
                    //where -1 is for not counting the first upgrader's request since the lock count for the first upgrader's request
                    //is already counted.
                    weakerModeLockCount = entityInfoManager.getDatasetLockCount(entityInfo);
                    entityInfoManager.setDatasetLockMode(entityInfo, lockMode);
                    entityInfoManager.increaseDatasetLockCount(entityInfo, waiterCount - 1);

                    if (entityHashValue == -1) { //dataset-granule lock
                        dLockInfo.increaseLockCount(LockMode.X, weakerModeLockCount + waiterCount - 1);//new lock mode
                        dLockInfo.decreaseLockCount(LockMode.S, weakerModeLockCount);//current lock mode
                    } else {
                        dLockInfo.increaseLockCount(LockMode.IX, weakerModeLockCount + waiterCount - 1);
                        dLockInfo.decreaseLockCount(LockMode.IS, weakerModeLockCount);
                    }
                }
            } else { //duplicated call
                entityInfoManager.increaseDatasetLockCount(entityInfo);
                if (entityHashValue == -1) { //dataset-granule
                    dLockInfo.increaseLockCount(datasetLockMode);
                } else { //entity-granule
                    dLockInfo.increaseLockCount(datasetLockMode);
                }
            }
        }

        return entityInfo;
    }

    private void lockEntityGranule(DatasetId datasetId, int entityHashValue, byte lockMode, int entityInfoFromDLockInfo,
            TransactionContext txnContext) throws ACIDException {
        JobId jobId = txnContext.getJobId();
        int jId = jobId.getId(); //int-type jobId
        int waiterObjId;
        int eLockInfo = -1;
        int entityInfo;
        DatasetLockInfo dLockInfo;
        JobInfo jobInfo;
        boolean isUpgrade = false;
        int waiterCount = 0;
        int weakerModeLockCount;

        dLockInfo = datasetResourceHT.get(datasetId);
        jobInfo = jobHT.get(jobId);
        eLockInfo = dLockInfo.getEntityResourceHT().get(entityHashValue);

        if (eLockInfo != -1) {
            //check duplicated call

            //1. lock request causing duplicated upgrading requests from different threads in a same job
            waiterObjId = entityLockInfoManager.findUpgraderFromUpgraderList(eLockInfo, jId, entityHashValue);
            if (waiterObjId != -1) {
                entityInfo = lockWaiterManager.getLockWaiter(waiterObjId).getEntityInfoSlot();
                waiterCount = handleLockWaiter(null, eLockInfo, -1, true, false, txnContext, jobInfo, waiterObjId);

                if (waiterCount > 0) {
                    weakerModeLockCount = entityInfoManager.getEntityLockCount(entityInfo);
                    entityInfoManager.setEntityLockMode(entityInfo, lockMode);
                    entityInfoManager.increaseEntityLockCount(entityInfo, waiterCount - 1);

                    entityLockInfoManager.increaseLockCount(entityInfo, LockMode.X, (short) (weakerModeLockCount
                            + waiterCount - 1));//new lock mode
                    entityLockInfoManager.decreaseLockCount(entityInfo, LockMode.S, (short) weakerModeLockCount);//old lock mode 
                }
                return;
            }

            //2. lock request causing duplicated waiting requests from different threads in a same job
            waiterObjId = entityLockInfoManager.findWaiterFromWaiterList(eLockInfo, jId, entityHashValue);
            if (waiterObjId != -1) {
                entityInfo = lockWaiterManager.getLockWaiter(waiterObjId).getEntityInfoSlot();
                waiterCount = handleLockWaiter(null, eLockInfo, -1, false, false, txnContext, jobInfo, waiterObjId);

                if (waiterCount > 0) {
                    entityInfoManager.increaseEntityLockCount(entityInfo, waiterCount);
                    entityLockInfoManager.increaseLockCount(eLockInfo, lockMode, (short) waiterCount);
                    entityLockInfoManager.addHolder(eLockInfo, entityInfo);
                }
                return;
            }

            //3. lock request causing duplicated holding requests from different threads or a single thread in a same job
            entityInfo = entityLockInfoManager.findEntityInfoFromHolderList(eLockInfo, jId, entityHashValue);
            if (entityInfo != -1) {//duplicated call or upgrader

                isUpgrade = isLockUpgrade(entityInfoManager.getEntityLockMode(entityInfo), lockMode);
                if (isUpgrade) {//upgrade call
                    //wait if any upgrader exists or upgrading lock mode is not compatible
                    if (entityLockInfoManager.getUpgrader(eLockInfo) != -1
                            || !entityLockInfoManager.isUpgradeCompatible(eLockInfo, lockMode, entityInfo)) {
                        waiterCount = handleLockWaiter(null, eLockInfo, entityInfo, true, false, txnContext, jobInfo,
                                -1);
                    } else {
                        waiterCount = 1;
                    }

                    if (waiterCount > 0) {
                        weakerModeLockCount = entityInfoManager.getEntityLockCount(entityInfo);
                        entityInfoManager.setEntityLockMode(entityInfo, lockMode);
                        entityInfoManager.increaseEntityLockCount(entityInfo, waiterCount - 1);

                        entityLockInfoManager.increaseLockCount(entityInfo, LockMode.X, (short) (weakerModeLockCount
                                + waiterCount - 1));//new lock mode
                        entityLockInfoManager.decreaseLockCount(entityInfo, LockMode.S, (short) weakerModeLockCount);//old lock mode 
                    }

                } else {//duplicated call
                    entityInfoManager.increaseEntityLockCount(entityInfo);
                    entityLockInfoManager.increaseLockCount(eLockInfo, lockMode);
                }
            } else {//new call from this job, but still eLockInfo exists since other threads hold it or wait on it
                entityInfo = entityInfoFromDLockInfo;
                if (entityLockInfoManager.getUpgrader(eLockInfo) != -1
                        || !entityLockInfoManager.isUpgradeCompatible(eLockInfo, lockMode, entityInfo)) {
                    waiterCount = handleLockWaiter(null, eLockInfo, entityInfo, false, false, txnContext, jobInfo, -1);
                } else {
                    waiterCount = 1;
                }

                if (waiterCount > 0) {
                    entityInfoManager.increaseEntityLockCount(entityInfo, waiterCount);
                    entityLockInfoManager.increaseLockCount(eLockInfo, lockMode, (short) waiterCount);
                    entityLockInfoManager.addHolder(eLockInfo, entityInfo);
                }
            }
        } else {//eLockInfo doesn't exist, so this lock request is the first request and can be granted without waiting.
            eLockInfo = entityLockInfoManager.allocate();
            dLockInfo.getEntityResourceHT().put(entityHashValue, eLockInfo);
            entityInfoManager.increaseEntityLockCount(entityInfoFromDLockInfo);
            entityLockInfoManager.increaseLockCount(eLockInfo, lockMode);
            entityLockInfoManager.addHolder(eLockInfo, entityInfoFromDLockInfo);
        }
    }

    @Override
    public void unlock(DatasetId datasetId, int entityHashValue, TransactionContext txnContext) throws ACIDException {
        JobId jobId = txnContext.getJobId();
        int eLockInfo = -1;
        DatasetLockInfo dLockInfo;
        JobInfo jobInfo;
        int entityInfo = -1;

        if (IS_DEBUG_MODE) {
            if (entityHashValue == -1) {
                throw new UnsupportedOperationException(
                        "Unsupported unlock request: dataset-granule unlock is not supported");
            }
        }

        latchLockTable();

        //find the resource to be unlocked
        dLockInfo = datasetResourceHT.get(datasetId);
        jobInfo = jobHT.get(jobId);
        if (IS_DEBUG_MODE) {
            if (dLockInfo == null || jobInfo == null) {
                throw new IllegalStateException("Invalid unlock request: Corresponding lock info doesn't exist.");
            }
        }
        eLockInfo = dLockInfo.getEntityResourceHT().get(entityHashValue);
        if (IS_DEBUG_MODE) {
            if (eLockInfo == -1) {
                throw new IllegalStateException("Invalid unlock request: Corresponding lock info doesn't exist.");
            }
        }

        //find the corresponding entityInfo
        entityInfo = entityLockInfoManager.findEntityInfoFromHolderList(eLockInfo, jobId.getId(), entityHashValue);
        if (IS_DEBUG_MODE) {
            if (entityInfo == -1) {
                throw new IllegalStateException("Invalid unlock request: Corresponding lock info doesn't exist.");
            }
        }

        //decrease the corresponding count of dLockInfo/eLockInfo/entityInfo
        dLockInfo.decreaseLockCount(entityInfoManager.getDatasetLockMode(entityInfo) == LockMode.S ? LockMode.IS
                : LockMode.IX);
        entityLockInfoManager.decreaseLockCount(eLockInfo, entityInfoManager.getEntityLockMode(entityInfo));
        entityInfoManager.decreaseDatasetLockCount(entityInfo);
        entityInfoManager.decreaseEntityLockCount(entityInfo);

        if (entityInfoManager.getEntityLockCount(entityInfo) == 0
                && entityInfoManager.getDatasetLockCount(entityInfo) == 0) {
            int threadCount = 0; //number of threads(in the same job) waiting on the same resource 
            int waiterObjId = jobInfo.getLastWaitingResource();
            int waitingEntityInfo;
            LockWaiter waiterObj;

            //1) wake up waiters and remove holder
            //wake up waiters of dataset-granule lock
            wakeUpDatasetLockWaiters(dLockInfo);
            //wake up waiters of entity-granule lock
            wakeUpEntityLockWaiters(eLockInfo);
            //remove the holder from eLockInfo's holder list and remove the holding resource from jobInfo's holding resource list
            //this can be done in the following single function call.
            entityLockInfoManager.removeHolder(eLockInfo, entityInfo, jobInfo);
            
            //2) if 
            //      there is no waiting thread on the same resource (this can be checked through jobInfo)
            //   then 
            //      a) delete the corresponding entityInfo
            //      b) write commit log for the unlocked resource(which is a committed txn).
            while (waiterObjId != -1) {
                waiterObj = lockWaiterManager.getLockWaiter(waiterObjId);
                waitingEntityInfo = waiterObj.getEntityInfoSlot();
                if (entityInfoManager.getDatasetId(waitingEntityInfo) == datasetId.getId()
                        && entityInfoManager.getPKHashVal(waitingEntityInfo) == entityHashValue) {
                    threadCount++;
                    break;
                }
                waiterObjId = entityInfoManager.getPrevJobResource(waitingEntityInfo);
            }
            if (threadCount == 0) {
                entityInfoManager.deallocate(entityInfo);
                //TODO
                //write commit log for the unlocked resource
            }
        }

        //deallocate entityLockInfo's slot if there is no txn referring to the entityLockInfo.
        if (entityLockInfoManager.getFirstWaiter(eLockInfo) == -1
                && entityLockInfoManager.getLastHolder(eLockInfo) == -1
                && entityLockInfoManager.getUpgrader(eLockInfo) == -1) {
            entityLockInfoManager.deallocate(eLockInfo);
        }

        //we don't deallocate datasetLockInfo even if there is no txn referring to the datasetLockInfo
        //since the datasetLockInfo is likely to be referred to again.

        unlatchLockTable();
    }

    @Override
    public synchronized void releaseLocks(TransactionContext txnContext) throws ACIDException {
        //TODO
    }

    @Override
    public void getInstantlock(DatasetId datasetId, int entityHashValue, byte lockMode, TransactionContext context)
            throws ACIDException {
        //TODO
        throw new ACIDException("Instant Locking is not supported");
    }

    @Override
    public boolean tryLock(DatasetId datasetId, int entityHashValue, byte lockMode, TransactionContext context)
            throws ACIDException {
        // TODO Auto-generated method stub
        return false;
    }

    private void latchLockTable() {
        lockTableLatch.writeLock().lock();
    }

    private void unlatchLockTable() {
        lockTableLatch.writeLock().unlock();
    }

    private void latchWaitNotify() {
        waiterLatch.writeLock().lock();
    }

    private void unlatchWaitNotify() {
        waiterLatch.writeLock().unlock();
    }

    private int handleLockWaiter(DatasetLockInfo dLockInfo, int eLockInfo, int entityInfo, boolean isUpgrade,
            boolean isDatasetLockInfo, TransactionContext txnContext, JobInfo jobInfo, int duplicatedWaiterObjId)
            throws ACIDException {
        int waiterId = -1;
        LockWaiter waiter;
        int waiterCount = 0;

        if (duplicatedWaiterObjId != -1 || isDeadlockFree(dLockInfo, eLockInfo, entityInfo, isDatasetLockInfo)) {//deadlock free -> wait
            if (duplicatedWaiterObjId == -1) {
                waiterId = lockWaiterManager.allocate(); //initial value of waiterObj: wait = true, victim = false
                waiter = lockWaiterManager.getLockWaiter(waiterId);
                waiter.setEntityInfoSlot(entityInfo);
                if (!isUpgrade && isDatasetLockInfo) {
                    //[Notice]
                    //upgrader was already added to the holding resource list
                    //entityLockInfo's waiter means that when the corresponding datasetLock is acquired,
                    //the corresponding entityInfo is already added to the holding resource list.
                    //Therefore, only the (non-upgrading)waiting datasetLock request is added to 
                    //the waiting resource list of JobInfo
                    jobInfo.addWaitingResource(waiterId);
                }
                txnContext.setStartWaitTime(System.currentTimeMillis());
            } else {
                waiterId = duplicatedWaiterObjId;
                waiter = lockWaiterManager.getLockWaiter(waiterId);
            }

            if (duplicatedWaiterObjId == -1) {
                //add actor properly
                if (isDatasetLockInfo) {
                    if (isUpgrade) {
                        dLockInfo.addUpgrader(waiterId);
                    } else {
                        dLockInfo.addWaiter(waiterId);
                    }
                } else {
                    if (isUpgrade) {
                        entityLockInfoManager.addUpgrader(eLockInfo, waiterId);
                    } else {
                        entityLockInfoManager.addWaiter(eLockInfo, waiterId);
                    }
                }
            }
            waiter.increaseWaiterCount();
            waiter.setFirstGetUp(true);

            latchWaitNotify();
            unlatchLockTable();
            synchronized (waiter) {
                unlatchWaitNotify();
                while (waiter.getWait()) {
                    try {
                        waiter.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }

            //waiter woke up -> remove/deallocate waiter object and abort if timeout
            latchLockTable();
            
            if (txnContext.getStatus() == TransactionContext.TIMED_OUT_SATUS) {
                try {
                    requestAbort(txnContext);
                } finally {
                    unlatchLockTable();
                }
            }
            
            if (waiter.isFirstGetUp()) {
                waiter.setFirstGetUp(false);
                waiterCount = waiter.getWaiterCount();
            } else {
                waiterCount = 0;
            }

            waiter.decreaseWaiterCount();
            if (waiter.getWaiterCount() == 0) {
                //remove actor properly
                if (isDatasetLockInfo) {
                    if (isUpgrade) {
                        dLockInfo.removeUpgrader(waiterId);
                    } else {
                        dLockInfo.removeWaiter(waiterId);
                    }
                } else {
                    if (isUpgrade) {
                        entityLockInfoManager.removeUpgrader(eLockInfo, waiterId);
                    } else {
                        entityLockInfoManager.removeWaiter(eLockInfo, waiterId);
                    }
                }

                if (!isUpgrade && isDatasetLockInfo) {
                    jobInfo.removeWaitingResource(waiterId);
                }
                lockWaiterManager.deallocate(waiterId);
            }


        } else { //deadlock -> abort
            //[Notice]
            //Before requesting abort, the entityInfo for waiting datasetLock request is deallocated.
            if (!isUpgrade && isDatasetLockInfo) {
                //deallocate the entityInfo
                entityInfoManager.deallocate(entityInfo);
            }
            try {
                requestAbort(txnContext);
            } finally {
                unlatchLockTable();
            }
        }

        return waiterCount;
    }

    private boolean isDeadlockFree(DatasetLockInfo dLockInfo, int eLockInfo, int entityInfo, boolean isDatasetLockInfo) {
        return deadlockDetector.isSafeToAdd(dLockInfo, eLockInfo, entityInfo, isDatasetLockInfo);
    }

    private void requestAbort(TransactionContext txnContext) throws ACIDException {
        txnContext.setStatus(TransactionContext.TIMED_OUT_SATUS);
        txnContext.setStartWaitTime(TransactionContext.INVALID_TIME);
        throw new ACIDException("Transaction " + txnContext.getJobId()
                + " should abort (requested by the Lock Manager)");
    }

    /**
     * For now, upgrading lock granule from entity-granule to dataset-granule is not supported!!
     * 
     * @param fromLockMode
     * @param toLockMode
     * @return
     */
    private boolean isLockUpgrade(byte fromLockMode, byte toLockMode) {
        return fromLockMode == LockMode.S && toLockMode == LockMode.X;
    }

    /**
     * wake up upgraders first, then waiters.
     * Criteria to wake up upgraders: if the upgrading lock mode is compatible, then wake up the upgrader.
     * BTW, how do we know the upgrading lock mode? (we don't keep the upgrading lock mode and waiting lock mode
     */
    private void wakeUpDatasetLockWaiters(DatasetLockInfo dLockInfo) {
        boolean areAllUpgradersWakenUp = true;
        int waiterObjId = dLockInfo.getFirstUpgrader();
        int entityInfo;
        LockWaiter waiterObj;
        byte datasetLockMode;
        byte lockMode;

        //wake up upgraders
        while (waiterObjId != -1) {
            waiterObj = lockWaiterManager.getLockWaiter(waiterObjId);
            entityInfo = waiterObj.getEntityInfoSlot();
            datasetLockMode = entityInfoManager.getPKHashVal(entityInfo) == -1 ? LockMode.X : LockMode.IX;
            if (!dLockInfo.isUpgradeCompatible(datasetLockMode, entityInfo)) {
                areAllUpgradersWakenUp = false;
                break;
            }
            //compatible upgrader is waken up
            latchWaitNotify();
            synchronized (waiterObj) {
                unlatchWaitNotify();
                waiterObj.setWait(false);
                waiterObj.notifyAll();
            }
            waiterObjId = entityInfoManager.getNextEntityActor(entityInfo);
        }
        
        //wake up waiters
        if (areAllUpgradersWakenUp) {
            waiterObjId = dLockInfo.getFirstWaiter();
            while (waiterObjId != -1) {
                waiterObj = lockWaiterManager.getLockWaiter(waiterObjId);
                entityInfo = waiterObj.getEntityInfoSlot();
                lockMode = entityInfoManager.getDatasetLockMode(entityInfo);
                datasetLockMode = entityInfoManager.getPKHashVal(entityInfo) == -1 ?  lockMode : lockMode == LockMode.S ? LockMode.IS : LockMode.IX;
                if (!dLockInfo.isCompatible(datasetLockMode)) {
                    break;
                }
                //compatible waiter is waken up
                latchWaitNotify();
                synchronized (waiterObj) {
                    unlatchWaitNotify();
                    waiterObj.setWait(false);
                    waiterObj.notifyAll();
                }
                waiterObjId = entityInfoManager.getNextEntityActor(entityInfo);
            }
        }
    }
    
    private void wakeUpEntityLockWaiters(int eLockInfo) {
        boolean isUpgraderWakenUp = true;
        int waiterObjId = entityLockInfoManager.getUpgrader(eLockInfo);
        int entityInfo;
        LockWaiter waiterObj;
        byte entityLockMode;
        byte lockMode;

        //wake up upgraders
        if (waiterObjId != -1) {
            waiterObj = lockWaiterManager.getLockWaiter(waiterObjId);
            entityInfo = waiterObj.getEntityInfoSlot();
            if (!entityLockInfoManager.isUpgradeCompatible(eLockInfo, LockMode.IX, entityInfo)) {
                isUpgraderWakenUp = false;
            } else {
                //compatible upgrader is waken up
                latchWaitNotify();
                synchronized (waiterObj) {
                    unlatchWaitNotify();
                    waiterObj.setWait(false);
                    waiterObj.notifyAll();
                }
            }
        }
        
        //wake up waiters
        if (isUpgraderWakenUp) {
            waiterObjId = entityLockInfoManager.getFirstWaiter(eLockInfo);
            while (waiterObjId != -1) {
                waiterObj = lockWaiterManager.getLockWaiter(waiterObjId);
                entityInfo = waiterObj.getEntityInfoSlot();
                lockMode = entityInfoManager.getDatasetLockMode(entityInfo);
                entityLockMode = entityInfoManager.getEntityLockMode(entityInfo);
                if (!entityLockInfoManager.isCompatible(eLockInfo, entityLockMode)) {
                    break;
                }
                //compatible waiter is waken up
                latchWaitNotify();
                synchronized (waiterObj) {
                    unlatchWaitNotify();
                    waiterObj.setWait(false);
                    waiterObj.notifyAll();
                }
                waiterObjId = entityInfoManager.getNextEntityActor(entityInfo);
            }
        }
    }
    

    @Override
    public String prettyPrint() throws ACIDException {
        StringBuilder s = new StringBuilder("\n########### LockManager Status #############\n");
        return s + "\n";
    }

    public void sweepForTimeout() throws ACIDException {
        /*    synchronized (lmTables) {
                Iterator<Long> txrIt = lmTables.getIteratorOnTxrs();
                while (txrIt.hasNext()) {
                    long nextTxrID = txrIt.next();
                    TxrInfo nextTxrInfo = lmTables.getTxrInfo(nextTxrID);
                    if (toutDetector.isVictim(nextTxrInfo)) {
                        nextTxrInfo.getContext().setStatus(TransactionContext.TIMED_OUT_SATUS);
                        LockInfo nextLockInfo = lmTables.getLockInfo(nextTxrInfo.getWaitOnRid());
                        synchronized (nextLockInfo) {
                            WaitingInfo nextVictim = nextLockInfo.getWaitingOnObject(nextTxrID, LockInfo.ANY_LOCK_MODE);
                            nextVictim.setAsVictim();
                            toutDetector.addToVictimsList(nextVictim.getWaitingEntry());
                        }
                    }
                }
            }
                */
    }

}

/******************************************
 * datasetResourceHT
 ******************************************/
class DatasetId implements Serializable {
    int id;

    public DatasetId(int id) {
        this.id = id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }
};

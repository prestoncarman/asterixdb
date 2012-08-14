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
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;

import edu.uci.ics.asterix.transaction.management.exception.ACIDException;
import edu.uci.ics.asterix.transaction.management.service.transaction.ITransactionManager.TransactionState;
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
    public static final boolean IS_DEBUG_MODE = true;//false

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
    private DatasetId tempDatasetIdObj; //temporary object to avoid object creation

    private int tryLockDatasetGranuleRevertOperation;
    private int tryLockEntityGranuleRevertOperation;

    private LockRequestTracker lockRequestTracker; //for debugging

    public LockManager(TransactionProvider txnProvider) throws ACIDException {
        this.txnProvider = txnProvider;
        this.lockTableLatch = new ReentrantReadWriteLock(true);
        this.waiterLatch = new ReentrantReadWriteLock(true);
        this.jobHT = new HashMap<JobId, JobInfo>();
        this.datasetResourceHT = new HashMap<DatasetId, DatasetLockInfo>();
        this.entityInfoManager = new EntityInfoManager();
        this.lockWaiterManager = new LockWaiterManager();
        this.entityLockInfoManager = new EntityLockInfoManager(entityInfoManager, lockWaiterManager);
        this.deadlockDetector = new DeadlockDetector(jobHT, datasetResourceHT, entityLockInfoManager,
                entityInfoManager, lockWaiterManager);
        this.toutDetector = new TimeOutDetector(this);
        this.tempDatasetIdObj = new DatasetId(0);

        if (IS_DEBUG_MODE) {
            this.lockRequestTracker = new LockRequestTracker();
        }
    }

    @Override
    public void lock(DatasetId datasetId, int entityHashValue, byte lockMode, TransactionContext txnContext)
            throws ACIDException {
        internalLock(datasetId, entityHashValue, lockMode, txnContext);
    }

    private void internalLock(DatasetId datasetId, int entityHashValue, byte lockMode, TransactionContext txnContext)
            throws ACIDException {

        JobId jobId = txnContext.getJobId();
        int jId = jobId.getId(); //int-type jobId
        int dId = datasetId.getId(); //int-type datasetId
        int entityInfo;
        int eLockInfo = -1;
        DatasetLockInfo dLockInfo = null;
        JobInfo jobInfo;
        byte datasetLockMode = entityHashValue == -1 ? lockMode : lockMode == LockMode.S ? LockMode.IS : LockMode.IX;

        latchLockTable();
        validateJob(txnContext);

        if (IS_DEBUG_MODE) {
            trackLockRequest("Requested", RequestType.LOCK, datasetId, entityHashValue, lockMode, txnContext, dLockInfo, eLockInfo);
        }

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

            if (IS_DEBUG_MODE) {
                trackLockRequest("Granted", RequestType.LOCK, datasetId, entityHashValue, lockMode, txnContext, dLockInfo, eLockInfo);
            }
            
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

        if (IS_DEBUG_MODE) {
            trackLockRequest("Granted", RequestType.LOCK, datasetId, entityHashValue, lockMode, txnContext, dLockInfo, eLockInfo);
        }
        unlatchLockTable();
        return;
    }

    private void validateJob(TransactionContext txnContext) throws ACIDException {
        if (txnContext.getTxnState() == TransactionState.ABORTED) {
            unlatchLockTable();
            throw new ACIDException("" + txnContext.getJobId() + " is in ABORTED state.");
        } else if (txnContext.getStatus() == TransactionContext.TIMED_OUT_STATUS) {
            try {
                requestAbort(txnContext);
            } finally {
                unlatchLockTable();
            }
        }
    }

    private int lockDatasetGranule(DatasetId datasetId, int entityHashValue, byte lockMode,
            TransactionContext txnContext) throws ACIDException {
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
            //the thread updates lock count on behalf of the all other waiting threads.
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
            if (dLockInfo.getFirstUpgrader() != -1 || dLockInfo.getFirstWaiter() != -1
                    || !dLockInfo.isCompatible(datasetLockMode)) {
                waiterCount = handleLockWaiter(dLockInfo, -1, entityInfo, false, true, txnContext, jobInfo, -1);
            } else {
                waiterCount = 1;
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
                } else {
                    waiterCount = 1;
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

    private void lockEntityGranule(DatasetId datasetId, int entityHashValue, byte lockMode,
            int entityInfoFromDLockInfo, TransactionContext txnContext) throws ACIDException {
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
                        || entityLockInfoManager.getFirstWaiter(eLockInfo) != -1
                        || !entityLockInfoManager.isCompatible(eLockInfo, lockMode)) {
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
        DatasetLockInfo dLockInfo = null;
        JobInfo jobInfo;
        int entityInfo = -1;

        if (IS_DEBUG_MODE) {
            if (entityHashValue == -1) {
                throw new UnsupportedOperationException(
                        "Unsupported unlock request: dataset-granule unlock is not supported");
            }
        }

        latchLockTable();
        validateJob(txnContext);

        if (IS_DEBUG_MODE) {
            trackLockRequest("Requested", RequestType.UNLOCK, datasetId, entityHashValue, (byte) 0, txnContext, dLockInfo, eLockInfo);
        }

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
                throw new IllegalStateException("Invalid unlock request[" + jobId.getId() + "," + datasetId.getId()
                        + "," + entityHashValue + "]: Corresponding lock info doesn't exist.");
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
            int waiterObjId = jobInfo.getFirstWaitingResource();
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
                waiterObjId = waiterObj.getNextWaiterObjId();
            }
            if (threadCount == 0) {
                if (entityInfoManager.getEntityLockMode(entityInfo) == LockMode.X) {
                    //TODO
                    //write a commit log for the unlocked resource
                    //need to figure out that instantLock() also needs to write a commit log. 
                }
                entityInfoManager.deallocate(entityInfo);
            }
        }

        //deallocate entityLockInfo's slot if there is no txn referring to the entityLockInfo.
        if (entityLockInfoManager.getFirstWaiter(eLockInfo) == -1
                && entityLockInfoManager.getLastHolder(eLockInfo) == -1
                && entityLockInfoManager.getUpgrader(eLockInfo) == -1) {
            dLockInfo.getEntityResourceHT().remove(entityHashValue);
            entityLockInfoManager.deallocate(eLockInfo);
        }

        //we don't deallocate datasetLockInfo even if there is no txn referring to the datasetLockInfo
        //since the datasetLockInfo is likely to be referred to again.

        if (IS_DEBUG_MODE) {
            trackLockRequest("Granted", RequestType.UNLOCK, datasetId, entityHashValue, (byte) 0, txnContext, dLockInfo, eLockInfo);
        }
        unlatchLockTable();
    }

    @Override
    public synchronized void releaseLocks(TransactionContext txnContext) throws ACIDException {
        LockWaiter waiterObj;
        int entityInfo;
        int prevEntityInfo;
        int entityHashValue;
        DatasetLockInfo dLockInfo = null;
        int eLockInfo = -1;
        int did;//int-type dataset id
        int lockCount;
        byte lockMode;

        JobId jobId = txnContext.getJobId();

        latchLockTable();

        if (IS_DEBUG_MODE) {
            trackLockRequest("Requested", RequestType.RELEASE_LOCKS, new DatasetId(0), 0, (byte) 0, txnContext, dLockInfo, eLockInfo);
        }

        JobInfo jobInfo = jobHT.get(jobId);

        //remove waiterObj of JobInfo 
        //[Notice]
        //waiterObjs may exist if aborted thread is the caller of this function.
        //Even if there are the waiterObjs, there is no waiting thread on the objects. 
        //If the caller of this function is an aborted thread, it is guaranteed that there is no waiting threads
        //on the waiterObjs since when the aborted caller thread is waken up, all other waiting threads are
        //also waken up at the same time through 'notifyAll()' call.
        //In contrast, if the caller of this function is not an aborted thread, then there is no waiting object.
        int waiterObjId = jobInfo.getFirstWaitingResource();
        int nextWaiterObjId;
        while (waiterObjId != -1) {
            waiterObj = lockWaiterManager.getLockWaiter(waiterObjId);
            nextWaiterObjId = waiterObj.getNextWaiterObjId();
            entityInfo = waiterObj.getEntityInfoSlot();
            if (IS_DEBUG_MODE) {
                if (jobId.getId() != entityInfoManager.getJobId(entityInfo)) {
                    throw new IllegalStateException("JobInfo("+jobId+") has diffrent Job(JID:"+entityInfoManager.getJobId(entityInfo)+"'s lock request!!!");
                }
            }
            //1. deallocate waiterObj
            lockWaiterManager.deallocate(waiterObjId);

            //2. deallocate entityInfo only if this waiter is not an upgrader
            if (entityInfoManager.getDatasetLockCount(entityInfo) == 0
                    && entityInfoManager.getEntityLockCount(entityInfo) == 0) {
                entityInfoManager.deallocate(entityInfo);
            }
            waiterObjId = nextWaiterObjId;
        }

        //release holding resources
        entityInfo = jobInfo.getLastHoldingResource();
        while (entityInfo != -1) {
            prevEntityInfo = entityInfoManager.getPrevJobResource(entityInfo);

            //decrease lock count of datasetLock and entityLock
            did = entityInfoManager.getDatasetId(entityInfo);
            tempDatasetIdObj.setId(did);
            dLockInfo = datasetResourceHT.get(tempDatasetIdObj);
            entityHashValue = entityInfoManager.getPKHashVal(entityInfo);

            if (entityHashValue == -1) {
                //decrease datasetLockCount
                lockMode = entityInfoManager.getDatasetLockMode(entityInfo);
                lockCount = entityInfoManager.getDatasetLockCount(entityInfo);
                dLockInfo.decreaseLockCount(lockMode, lockCount);

                //wakeup waiters of datasetLock and remove holder from datasetLockInfo
                wakeUpDatasetLockWaiters(dLockInfo);

                //remove the holder from datasetLockInfo only if the lock is dataset-granule lock.
                //--> this also removes the holding resource from jobInfo               
                //(Because the IX and IS lock's holders are handled implicitly, 
                //those are not in the holder list of datasetLockInfo.)
                dLockInfo.removeHolder(entityInfo, jobInfo);
            } else {
                //decrease datasetLockCount
                lockMode = entityInfoManager.getDatasetLockMode(entityInfo);
                lockMode = lockMode == LockMode.S ? LockMode.IS : LockMode.IX;
                lockCount = entityInfoManager.getDatasetLockCount(entityInfo);
                dLockInfo.decreaseLockCount(lockMode, lockCount);

                //decrease entityLockCount
                lockMode = entityInfoManager.getEntityLockMode(entityInfo);
                lockCount = entityInfoManager.getEntityLockCount(entityInfo);
                eLockInfo = dLockInfo.getEntityResourceHT().get(entityHashValue);
                if(IS_DEBUG_MODE) {
                    if (eLockInfo < 0) {
                        System.out.println("eLockInfo:"+eLockInfo);
                    }
                }
                entityLockInfoManager.decreaseLockCount(eLockInfo, lockMode, (short) lockCount);

                //wakeup waiters of datasetLock and don't remove holder from datasetLockInfo
                wakeUpDatasetLockWaiters(dLockInfo);

                //wakeup waiters of entityLock
                wakeUpEntityLockWaiters(eLockInfo);

                //remove the holder from entityLockInfo 
                //--> this also removes the holding resource from jobInfo
                entityLockInfoManager.removeHolder(eLockInfo, entityInfo, jobInfo);

                //deallocate entityLockInfo if there is no holder and waiter.
                if (entityLockInfoManager.getLastHolder(eLockInfo) == -1
                        && entityLockInfoManager.getFirstWaiter(eLockInfo) == -1
                        && entityLockInfoManager.getUpgrader(eLockInfo) == -1) {
                    dLockInfo.getEntityResourceHT().remove(entityHashValue);
                    entityLockInfoManager.deallocate(eLockInfo);
//                    if (IS_DEBUG_MODE) {
//                        System.out.println("removed PK["+entityHashValue+"]");
//                    }
                }
            }

            //deallocate entityInfo
            entityInfoManager.deallocate(entityInfo);
//            if (IS_DEBUG_MODE) {
//                System.out.println("dellocate EntityInfo["+entityInfo+"]");
//            }

            entityInfo = prevEntityInfo;
        }

        //remove JobInfo
        jobHT.remove(jobId);

        if (IS_DEBUG_MODE) {
            trackLockRequest("Granted", RequestType.RELEASE_LOCKS, new DatasetId(0), 0, (byte) 0, txnContext, dLockInfo, eLockInfo);
        }
        unlatchLockTable();
    }

    @Override
    public void instantLock(DatasetId datasetId, int entityHashValue, byte lockMode, TransactionContext txnContext)
            throws ACIDException {
        
//        try {
//            internalLock(datasetId, entityHashValue, lockMode, txnContext);
//            return;
//        } finally {
//            unlock(datasetId, entityHashValue, txnContext);
//        }
        internalLock(datasetId, entityHashValue, lockMode, txnContext);
        unlock(datasetId, entityHashValue, txnContext);
    }

    @Override
    public boolean tryLock(DatasetId datasetId, int entityHashValue, byte lockMode, TransactionContext txnContext)
            throws ACIDException {
        return internalTryLock(datasetId, entityHashValue, lockMode, txnContext);
    }

    @Override
    public boolean instantTryLock(DatasetId datasetId, int entityHashValue, byte lockMode, TransactionContext txnContext)
            throws ACIDException {
        boolean isGranted = false;
        //        try {
        //            isGranted = internalTryLock(datasetId, entityHashValue, lockMode, txnContext);
        //            return isGranted;
        //        } finally {
        //            if (isGranted) {
        //                unlock(datasetId, entityHashValue, txnContext);
        //            }
        //        }
        isGranted = internalTryLock(datasetId, entityHashValue, lockMode, txnContext);
        if (isGranted) {
            unlock(datasetId, entityHashValue, txnContext);
        }
        return isGranted;
    }

    private boolean internalTryLock(DatasetId datasetId, int entityHashValue, byte lockMode,
            TransactionContext txnContext) throws ACIDException {
        JobId jobId = txnContext.getJobId();
        int jId = jobId.getId(); //int-type jobId
        int dId = datasetId.getId(); //int-type datasetId
        int entityInfo;
        int eLockInfo = -1;
        DatasetLockInfo dLockInfo = null;
        JobInfo jobInfo;
        byte datasetLockMode = entityHashValue == -1 ? lockMode : lockMode == LockMode.S ? LockMode.IS : LockMode.IX;
        boolean isSuccess = false;

        latchLockTable();
        validateJob(txnContext);

        if (IS_DEBUG_MODE) {
            trackLockRequest("Requested", RequestType.TRY_LOCK, datasetId, entityHashValue, lockMode, txnContext, dLockInfo, eLockInfo);
        }

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
            
            if (IS_DEBUG_MODE) {
                trackLockRequest("Granted", RequestType.TRY_LOCK, datasetId, entityHashValue, lockMode, txnContext, dLockInfo, eLockInfo);
            }

            unlatchLockTable();
            return true;
        }

        //#. the datasetLockInfo exists in datasetResourceHT.
        //1. handle dataset-granule lock
        tryLockDatasetGranuleRevertOperation = 0;
        entityInfo = tryLockDatasetGranule(datasetId, entityHashValue, lockMode, txnContext);
        if (entityInfo == -2) {//-2 represents fail
            isSuccess = false;
        } else {
            //2. handle entity-granule lock
            if (entityHashValue != -1) {
                isSuccess = tryLockEntityGranule(datasetId, entityHashValue, lockMode, entityInfo, txnContext);
                if (!isSuccess) {
                    revertTryLockDatasetGranuleOperation(datasetId, entityHashValue, lockMode, entityInfo, txnContext);
                }
            }
        }
        
        if (IS_DEBUG_MODE) {
            if (isSuccess) {
                trackLockRequest("Granted", RequestType.TRY_LOCK, datasetId, entityHashValue, lockMode, txnContext, dLockInfo, eLockInfo);
            } else {
                trackLockRequest("Failed", RequestType.TRY_LOCK, datasetId, entityHashValue, lockMode, txnContext, dLockInfo, eLockInfo);
            }
        }
        
        unlatchLockTable();

        return isSuccess;
    }

    private void trackLockRequest(String msg, int requestType, DatasetId datasetIdObj, int entityHashValue, byte lockMode,
            TransactionContext txnContext, DatasetLockInfo dLockInfo, int eLockInfo) {
        StringBuilder s = new StringBuilder();
        LockRequest request = new LockRequest(Thread.currentThread().getName(), requestType, datasetIdObj, entityHashValue, lockMode, txnContext);
        s.append(msg);
        if (msg.equals("Granted")) {
            if (dLockInfo != null) {
                s.append("\t|D| ");
                s.append(dLockInfo.getIXCount()).append(",");
                s.append(dLockInfo.getISCount()).append(",");
                s.append(dLockInfo.getXCount()).append(",");
                s.append(dLockInfo.getSCount()).append(",");
                if (dLockInfo.getFirstUpgrader() != -1) {
                    s.append("+");
                } else {
                    s.append("-");
                }
                s.append(",");
                if (dLockInfo.getFirstWaiter() != -1) {
                    s.append("+");
                } else {
                    s.append("-");
                }
            }
            
            if (eLockInfo != -1) {
                s.append("\t|E| ");
                s.append(entityLockInfoManager.getXCount(eLockInfo)).append(",");
                s.append(entityLockInfoManager.getSCount(eLockInfo)).append(",");
                if (entityLockInfoManager.getUpgrader(eLockInfo) != -1) {
                    s.append("+");
                } else {
                    s.append("-");
                }
                s.append(",");
                if (entityLockInfoManager.getFirstWaiter(eLockInfo) != -1) {
                    s.append("+");
                } else {
                    s.append("-");
                }
            }
        }
        
        
        lockRequestTracker.addEvent(s.toString(), request);
        System.out.println(request.prettyPrint() + "--> " + s.toString());
    }

    public String getGlobalRequestHistory() {
        if (IS_DEBUG_MODE) {
            return lockRequestTracker.getGlobalRequestHistory();
        }
        return null;
    }

    public String getLocalRequestHistory() {
        if (IS_DEBUG_MODE) {
            return lockRequestTracker.getLocalRequestHistory();
        }
        return null;
    }

    private void revertTryLockDatasetGranuleOperation(DatasetId datasetId, int entityHashValue, byte lockMode,
            int entityInfo, TransactionContext txnContext) {
        JobId jobId = txnContext.getJobId();
        DatasetLockInfo dLockInfo;
        JobInfo jobInfo;
        int lockCount;
        byte datasetLockMode = entityHashValue == -1 ? lockMode : lockMode == LockMode.S ? LockMode.IS : LockMode.IX;

        dLockInfo = datasetResourceHT.get(datasetId);
        jobInfo = jobHT.get(jobId);

        //see tryLockDatasetGranule() function to know the revert operation
        switch (tryLockDatasetGranuleRevertOperation) {

            case 1://[revertOperation1]: reverting 'adding a holder'
                
                if (entityHashValue == -1) {
                    dLockInfo.decreaseLockCount(datasetLockMode);
                    dLockInfo.removeHolder(entityInfo, jobInfo); //--> this call removes entityInfo from JobInfo's holding-resource-list as well.
                } else {
                    dLockInfo.decreaseLockCount(datasetLockMode);
                    jobInfo.removeHoldingResource(entityInfo);
                }
                entityInfoManager.decreaseDatasetLockCount(entityInfo);
                if (jobInfo.getLastHoldingResource() == -1 && jobInfo.getFirstWaitingResource() == -1) {
                    jobHT.remove(jobId);
                }
                entityInfoManager.deallocate(entityInfo);
                break;

            case 2://[revertOperation2]: reverting 'adding an upgrader'
                lockCount = entityInfoManager.getDatasetLockCount(entityInfo);
                if (entityHashValue == -1) { //dataset-granule lock
                    dLockInfo.decreaseLockCount(LockMode.X, lockCount);
                    dLockInfo.increaseLockCount(LockMode.S, lockCount);
                } else {
                    dLockInfo.decreaseLockCount(LockMode.IX, lockCount);
                    dLockInfo.increaseLockCount(LockMode.IS, lockCount);
                }
                entityInfoManager.setDatasetLockMode(entityInfo, LockMode.S);
                break;

            case 3://[revertOperation3]: reverting 'adding a duplicated call'
                entityInfoManager.decreaseDatasetLockCount(entityInfo);
                if (entityHashValue == -1) { //dataset-granule
                    dLockInfo.decreaseLockCount(datasetLockMode);
                } else { //entity-granule
                    dLockInfo.decreaseLockCount(datasetLockMode);
                }

                break;
            default:
                //do nothing;
        }
    }

    private int tryLockDatasetGranule(DatasetId datasetId, int entityHashValue, byte lockMode,
            TransactionContext txnContext) throws ACIDException {
        JobId jobId = txnContext.getJobId();
        int jId = jobId.getId(); //int-type jobId
        int dId = datasetId.getId(); //int-type datasetId
        int waiterObjId;
        int entityInfo = -1;
        DatasetLockInfo dLockInfo;
        JobInfo jobInfo;
        boolean isUpgrade = false;
        int weakerModeLockCount;
        byte datasetLockMode = entityHashValue == -1 ? lockMode : lockMode == LockMode.S ? LockMode.IS : LockMode.IX;

        dLockInfo = datasetResourceHT.get(datasetId);
        jobInfo = jobHT.get(jobId);

        //check duplicated call

        //1. lock request causing duplicated upgrading requests from different threads in a same job
        waiterObjId = dLockInfo.findUpgraderFromUpgraderList(jId, entityHashValue);
        if (waiterObjId != -1) {
            return -2;
        }

        //2. lock request causing duplicated waiting requests from different threads in a same job
        waiterObjId = dLockInfo.findWaiterFromWaiterList(jId, entityHashValue);
        if (waiterObjId != -1) {
            return -2;
        }

        //3. lock request causing duplicated holding requests from different threads or a single thread in a same job
        entityInfo = dLockInfo.findEntityInfoFromHolderList(jId, entityHashValue);
        if (entityInfo == -1) { //new call from this job -> doesn't mean that eLockInfo doesn't exist since another thread might have create the eLockInfo already.

            //return fail if any upgrader exists or upgrading lock mode is not compatible
            if (dLockInfo.getFirstUpgrader() != -1 || dLockInfo.getFirstWaiter() != -1
                    || !dLockInfo.isCompatible(datasetLockMode)) {
                return -2;
            }

            //////////////////////////////////////////////////////////////////////////////////////
            //revert the following operations if the caller thread has to wait during this call.
            //[revertOperation1]
            entityInfo = entityInfoManager.allocate(jId, dId, entityHashValue, lockMode);
            if (jobInfo == null) {
                jobInfo = new JobInfo(entityInfoManager, lockWaiterManager, txnContext);
                jobHT.put(jobId, jobInfo);
            }

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

            //set revert operation to be reverted when tryLock() fails
            tryLockDatasetGranuleRevertOperation = 1;
            //////////////////////////////////////////////////////////////////////////////////////

        } else {
            isUpgrade = isLockUpgrade(entityInfoManager.getDatasetLockMode(entityInfo), lockMode);
            if (isUpgrade) { //upgrade call 
                //return fail if any upgrader exists or upgrading lock mode is not compatible
                if (dLockInfo.getFirstUpgrader() != -1 || !dLockInfo.isUpgradeCompatible(datasetLockMode, entityInfo)) {
                    return -2;
                }

                //update entityInfo's dataset lock count and datasetLockInfo's lock count
                weakerModeLockCount = entityInfoManager.getDatasetLockCount(entityInfo);

                //////////////////////////////////////////////////////////////////////////////////////
                //revert the following operations if the caller thread has to wait during this call.
                //[revertOperation2]
                entityInfoManager.setDatasetLockMode(entityInfo, lockMode);

                if (entityHashValue == -1) { //dataset-granule lock
                    dLockInfo.increaseLockCount(LockMode.X, weakerModeLockCount);//new lock mode
                    dLockInfo.decreaseLockCount(LockMode.S, weakerModeLockCount);//current lock mode
                } else {
                    dLockInfo.increaseLockCount(LockMode.IX, weakerModeLockCount);
                    dLockInfo.decreaseLockCount(LockMode.IS, weakerModeLockCount);
                }
                tryLockDatasetGranuleRevertOperation = 2;
                //////////////////////////////////////////////////////////////////////////////////////

            } else { //duplicated call

                //////////////////////////////////////////////////////////////////////////////////////
                //revert the following operations if the caller thread has to wait during this call.
                //[revertOperation3]
                entityInfoManager.increaseDatasetLockCount(entityInfo);
                if (entityHashValue == -1) { //dataset-granule
                    dLockInfo.increaseLockCount(datasetLockMode);
                } else { //entity-granule
                    dLockInfo.increaseLockCount(datasetLockMode);
                }
                tryLockDatasetGranuleRevertOperation = 3;
                //////////////////////////////////////////////////////////////////////////////////////

            }
        }

        return entityInfo;
    }

    private boolean tryLockEntityGranule(DatasetId datasetId, int entityHashValue, byte lockMode,
            int entityInfoFromDLockInfo, TransactionContext txnContext) throws ACIDException {
        JobId jobId = txnContext.getJobId();
        int jId = jobId.getId(); //int-type jobId
        int waiterObjId;
        int eLockInfo = -1;
        int entityInfo;
        DatasetLockInfo dLockInfo;
        boolean isUpgrade = false;
        int waiterCount = 0;
        int weakerModeLockCount;

        dLockInfo = datasetResourceHT.get(datasetId);
        eLockInfo = dLockInfo.getEntityResourceHT().get(entityHashValue);

        if (eLockInfo != -1) {
            //check duplicated call

            //1. lock request causing duplicated upgrading requests from different threads in a same job
            waiterObjId = entityLockInfoManager.findUpgraderFromUpgraderList(eLockInfo, jId, entityHashValue);
            if (waiterObjId != -1) {
                return false;
            }

            //2. lock request causing duplicated waiting requests from different threads in a same job
            waiterObjId = entityLockInfoManager.findWaiterFromWaiterList(eLockInfo, jId, entityHashValue);
            if (waiterObjId != -1) {
                return false;
            }

            //3. lock request causing duplicated holding requests from different threads or a single thread in a same job
            entityInfo = entityLockInfoManager.findEntityInfoFromHolderList(eLockInfo, jId, entityHashValue);
            if (entityInfo != -1) {//duplicated call or upgrader

                isUpgrade = isLockUpgrade(entityInfoManager.getEntityLockMode(entityInfo), lockMode);
                if (isUpgrade) {//upgrade call
                    //wait if any upgrader exists or upgrading lock mode is not compatible
                    if (entityLockInfoManager.getUpgrader(eLockInfo) != -1
                            || !entityLockInfoManager.isUpgradeCompatible(eLockInfo, lockMode, entityInfo)) {
                        return false;
                    }

                    weakerModeLockCount = entityInfoManager.getEntityLockCount(entityInfo);
                    entityInfoManager.setEntityLockMode(entityInfo, lockMode);

                    entityLockInfoManager.increaseLockCount(entityInfo, LockMode.X, (short) weakerModeLockCount);//new lock mode
                    entityLockInfoManager.decreaseLockCount(entityInfo, LockMode.S, (short) weakerModeLockCount);//old lock mode

                } else {//duplicated call
                    entityInfoManager.increaseEntityLockCount(entityInfo);
                    entityLockInfoManager.increaseLockCount(eLockInfo, lockMode);
                }
            } else {//new call from this job, but still eLockInfo exists since other threads hold it or wait on it
                entityInfo = entityInfoFromDLockInfo;
                if (entityLockInfoManager.getUpgrader(eLockInfo) != -1
                        || entityLockInfoManager.getFirstWaiter(eLockInfo) != -1
                        || !entityLockInfoManager.isCompatible(eLockInfo, lockMode)) {
                    return false;
                }

                entityInfoManager.increaseEntityLockCount(entityInfo, waiterCount);
                entityLockInfoManager.increaseLockCount(eLockInfo, lockMode, (short) waiterCount);
                entityLockInfoManager.addHolder(eLockInfo, entityInfo);
            }
        } else {//eLockInfo doesn't exist, so this lock request is the first request and can be granted without waiting.
            eLockInfo = entityLockInfoManager.allocate();
            dLockInfo.getEntityResourceHT().put(entityHashValue, eLockInfo);
            entityInfoManager.increaseEntityLockCount(entityInfoFromDLockInfo);
            entityLockInfoManager.increaseLockCount(eLockInfo, lockMode);
            entityLockInfoManager.addHolder(eLockInfo, entityInfoFromDLockInfo);
        }

        return true;
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
                    //TODO revisit this condition. waiterId should be added to JobInfo after acquired the DatasetLockInfo and waiting entityLockInfo.
                    jobInfo.addWaitingResource(waiterId);
                }
                waiter.setBeginWaitTime(System.currentTimeMillis());
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
                while (waiter.needWait()) {
                    try {
                        if (IS_DEBUG_MODE) {
                            System.out.println(""+Thread.currentThread().getName()+"\twaits("+waiter.getWaiterCount()+"): WID("+ waiterId + "),EID(" + waiter.getEntityInfoSlot()+")");
                        }
                        waiter.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }

            //waiter woke up -> remove/deallocate waiter object and abort if timeout
            latchLockTable();
            


            if (txnContext.getStatus() == TransactionContext.TIMED_OUT_STATUS || waiter.isVictim()) {
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
            if (IS_DEBUG_MODE) {
                System.out.println(""+Thread.currentThread().getName()+"\tgot-up!("+waiter.getWaiterCount()+"): WID("+ waiterId + "),EID(" + waiter.getEntityInfoSlot()+")");
            }
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
        txnContext.setStatus(TransactionContext.TIMED_OUT_STATUS);
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
     * TODO wake-up all compatible waiters instead of the first waiter.
     */
    private void wakeUpDatasetLockWaiters(DatasetLockInfo dLockInfo) {
        int waiterObjId = dLockInfo.getFirstUpgrader();
        int entityInfo;
        LockWaiter waiterObj;
        byte datasetLockMode;
        byte lockMode;

        if (waiterObjId != -1) {
            //wake up upgraders
            waiterObj = lockWaiterManager.getLockWaiter(waiterObjId);
            entityInfo = waiterObj.getEntityInfoSlot();
            datasetLockMode = entityInfoManager.getPKHashVal(entityInfo) == -1 ? LockMode.X : LockMode.IX;
            if (dLockInfo.isUpgradeCompatible(datasetLockMode, entityInfo)) {
                //compatible upgrader is waken up
                latchWaitNotify();
                synchronized (waiterObj) {
                    unlatchWaitNotify();
                    waiterObj.setWait(false);
                    if (IS_DEBUG_MODE) {
                        System.out.println(""+Thread.currentThread().getName()+"\twake-up(D): WID("+ waiterObjId + "),EID(" + waiterObj.getEntityInfoSlot()+")");
                    }
                    waiterObj.notifyAll();
                }
            }
        } else {
            //wake up waiters
            waiterObjId = dLockInfo.getFirstWaiter();
            if (waiterObjId != -1) {
                waiterObj = lockWaiterManager.getLockWaiter(waiterObjId);
                entityInfo = waiterObj.getEntityInfoSlot();
                lockMode = entityInfoManager.getDatasetLockMode(entityInfo);
                datasetLockMode = entityInfoManager.getPKHashVal(entityInfo) == -1 ? lockMode
                        : lockMode == LockMode.S ? LockMode.IS : LockMode.IX;
                if (dLockInfo.isCompatible(datasetLockMode)) {
                    //compatible waiter is waken up
                    latchWaitNotify();
                    synchronized (waiterObj) {
                        unlatchWaitNotify();
                        waiterObj.setWait(false);
                        if (IS_DEBUG_MODE) {
                            System.out.println(""+Thread.currentThread().getName()+"\twake-up(D): WID("+ waiterObjId + "),EID(" + waiterObj.getEntityInfoSlot()+")");
                        }
                        waiterObj.notifyAll();
                    }
                }
            }
        }
    }

    /**
     * TODO wake-up all compatible waiters instead of the first waiter.
     * @param eLockInfo
     */
    private void wakeUpEntityLockWaiters(int eLockInfo) {
        int waiterObjId = entityLockInfoManager.getUpgrader(eLockInfo);
        int entityInfo;
        LockWaiter waiterObj;
        byte entityLockMode;
        byte lockMode;

        if (waiterObjId != -1) {
            //wake up upgraders
            waiterObj = lockWaiterManager.getLockWaiter(waiterObjId);
            entityInfo = waiterObj.getEntityInfoSlot();
            if (entityLockInfoManager.isUpgradeCompatible(eLockInfo, LockMode.IX, entityInfo)) {
                latchWaitNotify();
                synchronized (waiterObj) {
                    unlatchWaitNotify();
                    waiterObj.setWait(false);
                    if (IS_DEBUG_MODE) {
                        System.out.println(""+Thread.currentThread().getName()+"\twake-up(E): WID("+ waiterObjId + "),EID(" + waiterObj.getEntityInfoSlot()+")");
                    }
                    waiterObj.notifyAll();
                }
            }
        } else {
            //wake up waiters
            waiterObjId = entityLockInfoManager.getFirstWaiter(eLockInfo);
            if (waiterObjId != -1) {
                waiterObj = lockWaiterManager.getLockWaiter(waiterObjId);
                entityInfo = waiterObj.getEntityInfoSlot();
                lockMode = entityInfoManager.getDatasetLockMode(entityInfo);
                entityLockMode = entityInfoManager.getEntityLockMode(entityInfo);
                if (entityLockInfoManager.isCompatible(eLockInfo, entityLockMode)) {
                    //compatible waiter is waken up
                    latchWaitNotify();
                    synchronized (waiterObj) {
                        unlatchWaitNotify();
                        waiterObj.setWait(false);
                        if (IS_DEBUG_MODE) {
                            System.out.println(""+Thread.currentThread().getName()+"\twake-up(E): WID("+ waiterObjId + "),EID(" + waiterObj.getEntityInfoSlot()+")");
                        }
                        waiterObj.notifyAll();
                    }
                }
            }
        }
    }

    @Override
    public String prettyPrint() throws ACIDException {
        StringBuilder s = new StringBuilder("\n########### LockManager Status #############\n");
        return s + "\n";
    }

    public void sweepForTimeout() throws ACIDException {
        JobInfo jobInfo;
        int waiterObjId;
        LockWaiter waiterObj;

        latchLockTable();

        Iterator<Entry<JobId, JobInfo>> iter = jobHT.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<JobId, JobInfo> pair = (Map.Entry<JobId, JobInfo>) iter.next();
            jobInfo = pair.getValue();
            waiterObjId = jobInfo.getFirstWaitingResource();
            while (waiterObjId != -1) {
                waiterObj = lockWaiterManager.getLockWaiter(waiterObjId);
                toutDetector.checkAndSetVictim(waiterObj);
                waiterObjId = waiterObj.getNextWaiterObjId();
            }
        }

        unlatchLockTable();
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

    @Override
    public int hashCode() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        if ((o == null) || !(o instanceof DatasetId)) {
            return false;
        }
        return ((DatasetId) o).id == this.id;
    }
};

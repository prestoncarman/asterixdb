package edu.uci.ics.asterix.transaction.management.service.locking;

import java.util.ArrayList;
import java.util.Random;

import edu.uci.ics.asterix.transaction.management.exception.ACIDException;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionContext;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionManagementConstants.LockManagerConstants.LockMode;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionProvider;

/**
 * LockManagerUnitTest: unit test of LockManager
 * 
 * @author kisskys
 */

public class LockManagerUnitTest {

    private static final int MAX_NUM_OF_UPGRADE_THREAD = 0;//2
    private static final int MAX_NUM_OF_ENTITY_LOCK_THREAD = 2;
    private static final int MAX_NUM_OF_DATASET_LOCK_THREAD = 0;
    private static final int MAX_NUM_OF_THREAD_IN_A_JOB = 4; //4
    private static int jobId = 0;
    private static Random rand;

    public static void main(String args[]) throws ACIDException {
        int i;
        TransactionProvider txnProvider = new TransactionProvider("LockManagerUnitTest");
        rand = new Random(100);
        for (i = 0; i < MAX_NUM_OF_ENTITY_LOCK_THREAD; i++) {
            generateEntityLockThread(txnProvider);
        }

        for (i = 0; i < MAX_NUM_OF_DATASET_LOCK_THREAD; i++) {
            generateDatasetLockThread(txnProvider);
        }

        for (i = 0; i < MAX_NUM_OF_UPGRADE_THREAD; i++) {
            generateEntityLockUpgradeThread(txnProvider);
        }
    }

    private static void generateEntityLockThread(TransactionProvider txnProvider) {
        Thread t;
        int childCount = 1;//rand.nextInt(MAX_NUM_OF_THREAD_IN_A_JOB);
        TransactionContext txnContext = generateTxnContext(txnProvider);

        for (int i = 0; i < childCount; i++) {
            t = new Thread(new LockRequestProducer(txnProvider.getLockManager(), txnContext, false, false, false));
            t.start();
        }
    }

    private static void generateDatasetLockThread(TransactionProvider txnProvider) {
        Thread t;
        int childCount = rand.nextInt(MAX_NUM_OF_THREAD_IN_A_JOB);
        TransactionContext txnContext = generateTxnContext(txnProvider);

        for (int i = 0; i < childCount; i++) {
            t = new Thread(new LockRequestProducer(txnProvider.getLockManager(), txnContext, true, false, false));
            t.start();
        }
    }

    private static void generateEntityLockUpgradeThread(TransactionProvider txnProvider) {
        Thread t;
        int childCount = MAX_NUM_OF_THREAD_IN_A_JOB;
        TransactionContext txnContext = generateTxnContext(txnProvider);

        for (int i = 0; i < childCount - 1; i++) {
            t = new Thread(new LockRequestProducer(txnProvider.getLockManager(), txnContext, false, true, false));
            t.start();
        }
        t = new Thread(new LockRequestProducer(txnProvider.getLockManager(), txnContext, false, true, true));
    }

    private static TransactionContext generateTxnContext(TransactionProvider txnProvider) {
        try {
            return new TransactionContext((long) (jobId++), txnProvider);
        } catch (ACIDException e) {
            e.printStackTrace();
            return null;
        }
    }

}

class LockRequestProducer implements Runnable {

    private static final long serialVersionUID = -3191274684985609965L;
    private static final int MAX_DATASET_NUM = 10;
    private static final int MAX_ENTITY_NUM = 100;
    private static final int MAX_LOCK_MODE_NUM = 2;
    private static final long DATASET_LOCK_THREAD_SLEEP_TIME = 1000;
    private static final int MAX_LOCK_REQUEST_TYPE_NUM = 4;

    private ILockManager lockMgr;
    private TransactionContext txnContext;
    private Random rand;
    private boolean isDatasetLock; //dataset or entity
    private ArrayList<LockRequest> requestQueue;
    private StringBuilder requestHistory;
    private int unlockIndex;
    private int upgradeIndex;
    private boolean isUpgradeThread;
    private boolean isUpgradeThreadJob;

    public LockRequestProducer(ILockManager lockMgr, TransactionContext txnContext, boolean isDatasetLock,
            boolean isUpgradeThreadJob, boolean isUpgradeThread) {
        this.lockMgr = lockMgr;
        this.txnContext = txnContext;
        this.isDatasetLock = isDatasetLock;
        this.isUpgradeThreadJob = isUpgradeThreadJob;
        this.isUpgradeThread = isUpgradeThread;

        this.rand = new Random(txnContext.getJobId().getId());
        requestQueue = new ArrayList<LockRequest>();
        requestHistory = new StringBuilder();
        unlockIndex = 0;
        upgradeIndex = 0;
    }

    @Override
    public void run() {
        if (isDatasetLock) {
            runDatasetLockTask();
        } else {
            runEntityLockTask();
        }
        System.out.println("" + Thread.currentThread().getName() + "\n" + requestHistory.toString() + ""
                + Thread.currentThread().getName() + "\n");
        System.out.println("GlobalRequestHistory\n"+((LockManager)lockMgr).getGlobalRequestHistory());
        System.out.println("");
        System.out.println("RequestHistoryPerJobId\n"+((LockManager)lockMgr).getLocalRequestHistory());
        System.out.println("");
    }

    private void runDatasetLockTask() {
        try {
            produceDatasetLockRequest();
        } catch (ACIDException e) {
            e.printStackTrace();
            return;
        }

        try {
            Thread.sleep(DATASET_LOCK_THREAD_SLEEP_TIME);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        try {
            produceDatasetUnlockRequest();
        } catch (ACIDException e) {
            e.printStackTrace();
            return;
        }
    }

    private void runEntityLockTask() {
        int i;
        byte lockMode;
        int lockCount;
        int upgradeCount;
        int releaseCount;

        lockCount = 1 + rand.nextInt(20);
        if (isUpgradeThreadJob) {
            if (isUpgradeThread) {
                upgradeCount = rand.nextInt(4) + 1;
                if (upgradeCount > lockCount) {
                    upgradeCount = lockCount;
                }
            } else {
                upgradeCount = 0;
            }
            lockMode = LockMode.S;
        } else {
            upgradeCount = 0;
            lockMode = (byte) (this.txnContext.getJobId().getId() % 2);
        }
        releaseCount = rand.nextInt(1000) % 13 == 0 ? 1 : 0;

        //lock
        for (i = 0; i < lockCount; i++) {
            try {
                produceEntityLockRequest(lockMode);
            } catch (ACIDException e) {
                e.printStackTrace();
                return;
            }
        }

        //upgrade
        for (i = 0; i < upgradeCount; i++) {
            try {
                produceEntityLockUpgradeRequest();
            } catch (ACIDException e) {
                e.printStackTrace();
                return;
            }
        }

        //unlock or releaseLocks
        if (releaseCount == 0) {
            //unlock
            for (i = 0; i < lockCount; i++) {
                try {
                    produceEntityUnlockRequest();
                } catch (ACIDException e) {
                    e.printStackTrace();
                    return;
                }
            }
        } else {
            try {
                produceEntityReleaseLocksRequest();
            } catch (ACIDException e) {
                e.printStackTrace();
                return;
            }
        }
    }

    private void produceDatasetLockRequest() throws ACIDException {
        int requestType = RequestType.LOCK;
        int datasetId = rand.nextInt(MAX_DATASET_NUM);
        int entityHashValue = -1;
        byte lockMode = (byte) (rand.nextInt(MAX_LOCK_MODE_NUM));
        LockRequest request = new LockRequest(requestType, new DatasetId(datasetId), entityHashValue, lockMode,
                txnContext);
        requestQueue.add(request);
        requestHistory.append(request.prettyPrint());
        sendRequest(request);
    }

    private void produceDatasetUnlockRequest() throws ACIDException {
        LockRequest lockRequest = requestQueue.get(0);

        int requestType = RequestType.RELEASE_LOCKS;
        int datasetId = lockRequest.datasetIdObj.getId();
        int entityHashValue = -1;
        byte lockMode = LockMode.S;//lockMode is not used for unlock() call.
        LockRequest request = new LockRequest(requestType, new DatasetId(datasetId), entityHashValue, lockMode,
                txnContext);
        requestQueue.add(request);
        requestHistory.append(request.prettyPrint());
        sendRequest(request);
    }

    private void produceEntityLockRequest(byte lockMode) throws ACIDException {
        int requestType = rand.nextInt(MAX_LOCK_REQUEST_TYPE_NUM);
        int datasetId = rand.nextInt(MAX_DATASET_NUM);
        int entityHashValue = rand.nextInt(MAX_ENTITY_NUM);
        LockRequest request = new LockRequest(requestType, new DatasetId(datasetId), entityHashValue, lockMode,
                txnContext);
        requestQueue.add(request);
        requestHistory.append(request.prettyPrint());
        sendRequest(request);
    }

    private void produceEntityLockUpgradeRequest() throws ACIDException {
        LockRequest lockRequest = null;
        int size = requestQueue.size();
        boolean existLockRequest = false;

        while (upgradeIndex < size) {
            lockRequest = requestQueue.get(upgradeIndex++);
            if (lockRequest.isUpgrade) {
                continue;
            }
            if (lockRequest.requestType == RequestType.UNLOCK || lockRequest.requestType == RequestType.RELEASE_LOCKS) {
                continue;
            }
            if (lockRequest.lockMode == LockMode.X) {
                continue;
            }
            existLockRequest = true;
            break;
        }

        if (existLockRequest) {
            int requestType = lockRequest.requestType;
            int datasetId = lockRequest.datasetIdObj.getId();
            int entityHashValue = lockRequest.entityHashValue;
            byte lockMode = LockMode.X;
            LockRequest request = new LockRequest(requestType, new DatasetId(datasetId), entityHashValue, lockMode,
                    txnContext);
            request.isUpgrade = true;
            requestQueue.add(request);
            requestHistory.append(request.prettyPrint());
            sendRequest(request);
        }
    }

    private void produceEntityUnlockRequest() throws ACIDException {
        LockRequest lockRequest = null;
        int size = requestQueue.size();
        boolean existLockRequest = false;

        while (unlockIndex < size) {
            lockRequest = requestQueue.get(unlockIndex++);
            if (lockRequest.isUpgrade) {
                continue;
            }
            if (lockRequest.requestType == RequestType.UNLOCK || lockRequest.requestType == RequestType.RELEASE_LOCKS
                    || lockRequest.requestType == RequestType.INSTANT_LOCK
                    || lockRequest.requestType == RequestType.INSTANT_TRY_LOCK) {
                continue;
            }
            existLockRequest = true;
            break;
        }

        if (existLockRequest) {
            int requestType = RequestType.UNLOCK;
            int datasetId = lockRequest.datasetIdObj.getId();
            int entityHashValue = lockRequest.entityHashValue;
            byte lockMode = lockRequest.lockMode;
            LockRequest request = new LockRequest(requestType, new DatasetId(datasetId), entityHashValue, lockMode,
                    txnContext);
            requestQueue.add(request);
            requestHistory.append(request.prettyPrint());
            sendRequest(request);
        }
    }

    private void produceEntityReleaseLocksRequest() throws ACIDException {
        LockRequest lockRequest = requestQueue.get(0);

        int requestType = RequestType.RELEASE_LOCKS;
        int datasetId = -1;
        int entityHashValue = -1;
        byte lockMode = LockMode.S;
        LockRequest request = new LockRequest(requestType, new DatasetId(datasetId), entityHashValue, lockMode,
                txnContext);
        requestQueue.add(request);
        requestHistory.append(request.prettyPrint());
        sendRequest(request);
    }

    private void sendRequest(LockRequest request) throws ACIDException {

        switch (request.requestType) {
            case RequestType.LOCK:
                lockMgr.lock(request.datasetIdObj, request.entityHashValue, request.lockMode, request.txnContext);
                break;
            case RequestType.INSTANT_LOCK:
                lockMgr.instantLock(request.datasetIdObj, request.entityHashValue, request.lockMode, request.txnContext);
                break;
            case RequestType.TRY_LOCK:
                lockMgr.tryLock(request.datasetIdObj, request.entityHashValue, request.lockMode, request.txnContext);
                break;
            case RequestType.INSTANT_TRY_LOCK:
                lockMgr.instantTryLock(request.datasetIdObj, request.entityHashValue, request.lockMode,
                        request.txnContext);
                break;
            case RequestType.UNLOCK:
                lockMgr.unlock(request.datasetIdObj, request.entityHashValue, request.txnContext);
                break;
            case RequestType.RELEASE_LOCKS:
                lockMgr.releaseLocks(request.txnContext);
                break;
            default:
                throw new UnsupportedOperationException("Unsupported lock method");
        }
    }
}

class LockRequest {
    public int requestType;
    public DatasetId datasetIdObj;
    public int entityHashValue;
    public byte lockMode;
    public TransactionContext txnContext;
    public boolean isUpgrade;

    public LockRequest(int requestType, DatasetId datasetIdObj, int entityHashValue, byte lockMode,
            TransactionContext txnContext) {
        this.requestType = requestType;
        this.datasetIdObj = datasetIdObj;
        this.entityHashValue = entityHashValue;
        this.lockMode = lockMode;
        this.txnContext = txnContext;
        isUpgrade = false;
    }

    public String prettyPrint() {
        StringBuilder s = new StringBuilder();
        switch (requestType) {
            case RequestType.LOCK:
                s.append("|L|");
                break;
            case RequestType.TRY_LOCK:
                s.append("|TL|");
                break;
            case RequestType.INSTANT_LOCK:
                s.append("|IL|");
                break;
            case RequestType.INSTANT_TRY_LOCK:
                s.append("|ITL|");
                break;
            case RequestType.UNLOCK:
                s.append("|UL|");
                break;
            case RequestType.RELEASE_LOCKS:
                s.append("|RL|");
                break;
            default:
                throw new UnsupportedOperationException("Unsupported method");
        }
        s.append("\t").append(txnContext.getJobId().getId()).append(",").append(datasetIdObj.getId()).append(",")
                .append(entityHashValue).append(":");
        switch (lockMode) {
            case LockMode.S:
                s.append("S");
                break;
            case LockMode.X:
                s.append("X");
                break;
            case LockMode.IS:
                s.append("IS");
                break;
            case LockMode.IX:
                s.append("IX");
                break;
            default:
                throw new UnsupportedOperationException("Unsupported lock mode");
        }
        s.append(",").append(isUpgrade).append("\n");
        return s.toString();
    }
}

class RequestType {
    public static final int LOCK = 0;
    public static final int TRY_LOCK = 1;
    public static final int INSTANT_LOCK = 2;
    public static final int INSTANT_TRY_LOCK = 3;
    public static final int UNLOCK = 4;
    public static final int RELEASE_LOCKS = 5;
}

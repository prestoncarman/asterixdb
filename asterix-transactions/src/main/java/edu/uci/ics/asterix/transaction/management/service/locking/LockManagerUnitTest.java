package edu.uci.ics.asterix.transaction.management.service.locking;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

import edu.uci.ics.asterix.transaction.management.exception.ACIDException;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionContext;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionManagementConstants.LockManagerConstants.LockMode;

/**
 * LockManagerUnitTest: unit test of LockManager
 * 
 * @author kisskys
 */

public class LockManagerUnitTest {

    public static void main(String args[]) throws ACIDException {
    

    }
}

class Txr extends TransactionContext implements Runnable {

    
    private static final long serialVersionUID = -3191274684985609965L;
    final double S_LOCK_PROB = 0.6;
    final double CONVERT_PROB = 0.5;

    int it; //how many times it cycles
    Random rand;
    ILockManager lm;
    TaskTracker tracker;
    String history;

    public Txr(long tid, int it, ILockManager lm, TaskTracker tracker) {
        super(tid);
        this.it = it;
        this.rand = new Random();
        this.lm = lm;
        this.tracker = tracker;
        this.history = "";
    }

    @Override
    public void run() {
        extendHistory("(it=" + it + ")\t");
        tracker.updateTxrHistory(getMyId(), history);
        for (int i = 0; i < it; i++) {
            //System.out.println("Iteration "+i+" for Transaction "+getTransactionID());
            byte[] resToLock = tracker.getOneRes();
            int mode = getLockReqMode();
            try {
                extendHistory(((mode == LockManagerTester.S_LOCKMODE) ? "R(S" : "R(X") + i + ")");
                tracker.updateTxrHistory(getMyId(), history);
                boolean req = lm.lock(this, resToLock, mode);
                if (!req) {
                    //System.out.println("Transaction "+getMyId()+" failed");
                    extendHistory("[Lock FAILED]");
                    tracker.updateTxrHistory(getMyId(), history);
                    //LockManagerTester.incKilledxr();
                    tracker.incKilledTXrs();
                    return;
                }
                extendHistory(((mode == LockManagerTester.S_LOCKMODE) ? "G(S" : "G(X") + i + ")");
                tracker.updateTxrHistory(getMyId(), history);

                Thread.sleep(500 + rand.nextInt(1500));

                if (mode == LockManagerTester.S_LOCKMODE) {
                    if (rand.nextDouble() < CONVERT_PROB) {
                        extendHistory("R(C" + "" + i + ")");
                        tracker.updateTxrHistory(getMyId(), history);
                        mode = LockManagerTester.X_LOCKMODE;
                        boolean c = lm.convertLock(this, resToLock, mode);
                        if (!c) {
                            //System.out.println("Transaction "+getMyId()+" failed in conversion");
                            extendHistory("[CONVERT FAILED]");
                            tracker.updateTxrHistory(getMyId(), history);
                            //LockManagerTester.incKilledxr();
                            tracker.incKilledTXrs();
                            return;
                        }
                        extendHistory("G(C" + "" + i + ")");
                        tracker.updateTxrHistory(getMyId(), history);
                    }
                }

                lm.unlock(this, resToLock);
                extendHistory(((mode == LockManagerTester.S_LOCKMODE) ? "U(S" : "U(X") + i + ")");
                tracker.updateTxrHistory(getMyId(), history);
                //tracker.decIt(getMyId(), history);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        extendHistory("(DONE)");
        tracker.updateTxrHistory(getMyId(), history);
        tracker.incFinishedTxrs(getMyId(), history);
        //LockManagerTester.incFinishedTxr();
        //tracker.incFinishedTxrs(getMyId(), history);
        System.out.println("\n" + tracker.getCountStat());
        System.out.println("Txr Table Size:\t" + ((LockManager) lm).dsAccessor.getTxrTableSize() + "\n");
    }

    private int getLockReqMode() {
        return ((rand.nextDouble() < S_LOCK_PROB) ? LockManagerTester.S_LOCKMODE : LockManagerTester.X_LOCKMODE);
    }

    private long getMyId() {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.putLong(getTransactionID());
        return buffer.getLong(0);
    }

    private void extendHistory(String next) {
        history += next + " -> ";
        //System.out.println("Transaction "+getMyId()+":\t"+history);
    }
}

class LockRequestTracker {
    ArrayList<LockRequest> allRequestList;
    HashMap<Integer, StringBuilder> requestMapPerJob;

    public LockRequestTracker() {
        allRequestList = new ArrayList<LockRequest>();
        requestMapPerJob = new HashMap<Integer, StringBuilder>();
    }
    
    public synchronized void addRequest(LockRequest request) {
        int jobId = request.txnContext.getJobId().getId();
        allRequestList.add(request);
        StringBuilder s = requestMapPerJob.get(jobId);
        if (s == null) {
            requestMapPerJob.put(request.txnContext.getJobId().getId(), new StringBuilder(request.prettyPrint()));
        } else {
            requestMapPerJob.put(request.txnContext.getJobId().getId(), s.append(request.prettyPrint()));
        }
    }
}

class LockRequest {
    public RequestType requestType;
    public DatasetId datasetIdObj;
    public int entityHashValue;
    public byte lockMode;
    public TransactionContext txnContext;

    public LockRequest(RequestType requestType, DatasetId datasetIdObj, int entityHashValue, byte lockMode,
            TransactionContext txnContext) {
        this.requestType = requestType;
        this.datasetIdObj = datasetIdObj;
        this.entityHashValue = entityHashValue;
        this.lockMode = lockMode;
        this.txnContext = txnContext;
    }

    public String prettyPrint() {
        StringBuilder s = new StringBuilder();
        switch (requestType) {
            case LOCK:
                s.append("|L|");
                break;
            case INSTANT_LOCK:
                s.append("|IL|");
                break;
            case TRY_LOCK:
                s.append("|TL|");
                break;
            case INSTANT_TRY_LOCK:
                s.append("|ITL|");
                break;
            case UNLOCK:
                s.append("|UL|");
                break;
            case RELEASE_LOCKS:
                s.append("|RL|");
                break;
            default:
                throw new UnsupportedOperationException("Unsupported method");
        }
        s.append("\t").append(txnContext.getJobId().getId()).append(",").append(datasetIdObj.getId()).append(",").append(entityHashValue).append(":");
        switch (lockMode) {
            case LockMode.IS:
                s.append("IS\n");
                break;
            case LockMode.IX:
                s.append("IX\n");
                break;
            case LockMode.S:
                s.append("S\n");
                break;
            case LockMode.X:
                s.append("X\n");
                break;
            default:
                throw new UnsupportedOperationException("Unsupported lock mode");
        }
        return s.toString();
    }
}

enum RequestType {
    LOCK,
    INSTANT_LOCK,
    TRY_LOCK,
    INSTANT_TRY_LOCK,
    UNLOCK,
    RELEASE_LOCKS
}

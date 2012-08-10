package edu.uci.ics.asterix.transaction.management.service.locking;

import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionContext;

public class JobInfo {
    private EntityInfoManager entityInfoManager;
    private LockWaiterManager lockWaiterManager;
    private TransactionContext jobCtx;
    private int lastHoldingResource; //resource(entity or dataset) which is held by this job lastly
    private int firstWaitingResource; //resource(entity or dataset) which this job is waiting for
    private int upgradingResource; //resource(entity or dataset) which this job is waiting for to upgrade

    public JobInfo(EntityInfoManager entityInfoManager, LockWaiterManager lockWaiterManager, TransactionContext txnCtx) {
        this.entityInfoManager = entityInfoManager;
        this.lockWaiterManager = lockWaiterManager;
        this.jobCtx = txnCtx;
        this.lastHoldingResource = -1;
        this.firstWaitingResource = -1;
        this.upgradingResource = -1;
    }

    public void addHoldingResource(int resource) {
        if (lastHoldingResource != -1) {
            entityInfoManager.setNextJobResource(lastHoldingResource, resource);
        }
        entityInfoManager.setPrevJobResource(resource, lastHoldingResource);
        entityInfoManager.setNextJobResource(resource, -1);
        lastHoldingResource = resource;
    }

    public void removeHoldingResource(int resource) {
        int current = lastHoldingResource;
        int prev;
        int next;

        while (current != resource) {

            if (LockManager.IS_DEBUG_MODE) {
                if (current == -1) {
                    //shouldn't occur: debugging purpose
                    try {
                        throw new Exception();
                    } catch (Exception e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
            }

            current = entityInfoManager.getPrevJobResource(current);
        }

        prev = entityInfoManager.getPrevJobResource(current);
        next = entityInfoManager.getNextJobResource(current);
        //update prev->next = next
        if (prev != -1) {
            entityInfoManager.setNextJobResource(prev, next);
        }
        if (next != -1) {
            entityInfoManager.setPrevJobResource(next, prev);
        }
        if (lastHoldingResource == resource) {
            lastHoldingResource = prev;
        }
    }
    
    public void addWaitingResource(int waiterObjId) {
        int lastObjId;
        LockWaiter lastObj = null;

        if (firstWaitingResource != -1) {
            //find the lastWaiter
            lastObjId = firstWaitingResource;
            while (lastObjId != -1) {
                lastObj = lockWaiterManager.getLockWaiter(lastObjId);
                lastObjId = lastObj.getNextWaiterObjId();
            }
            //last->next = new_waiter
            lastObj.setNextWaiterObjId(waiterObjId);
        } else {
            firstWaitingResource = waiterObjId;
        }
        //new_waiter->next = -1
        lastObj = lockWaiterManager.getLockWaiter(waiterObjId);
        lastObj.setNextWaiterObjId(-1);
    }

    public void removeWaitingResource(int waiterObjId) {
        int currentObjId = firstWaitingResource;
        LockWaiter currentObj;
        LockWaiter prevObj = null;
        int prevObjId = -1;
        int nextObjId;

        while (currentObjId != waiterObjId) {

            if (LockManager.IS_DEBUG_MODE) {
                if (currentObjId == -1) {
                    //shouldn't occur: debugging purpose
                    try {
                        throw new Exception();
                    } catch (Exception e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
            }

            prevObj = lockWaiterManager.getLockWaiter(currentObjId);
            prevObjId = currentObjId;
            currentObjId = prevObj.getNextWaiterObjId();
        }

        //get current waiter object
        currentObj = lockWaiterManager.getLockWaiter(currentObjId);

        //get next waiterObjId
        nextObjId = currentObj.getNextWaiterObjId();

        if (prevObjId != -1) {
            //prev->next = next
            prevObj.setNextWaiterObjId(nextObjId);
        } else {
            //removed first waiter. firstWaiter = current->next
            firstWaitingResource = nextObjId;
        }
    }

    /////////////////////////////////////////////////////////
    //  set/get method for private variable
    /////////////////////////////////////////////////////////
    public void setlastHoldingResource(int resource) {
        lastHoldingResource = resource;
    }

    public int getLastHoldingResource() {
        return lastHoldingResource;
    }

    public void setFirstWaitingResource(int resource) {
        firstWaitingResource = resource;
    }

    public int getFirstWaitingResource() {
        return firstWaitingResource;
    }

    public void setUpgradingResource(int resource) {
        upgradingResource = resource;
    }

    public int getUpgradingResource() {
        return upgradingResource;
    }
}

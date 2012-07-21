package edu.uci.ics.asterix.transaction.management.service.locking;

import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionContext;

public class JobInfo {
    private EntityInfoManager entityInfoManager;
    private LockWaiterManager lockWaiterManager;
    private TransactionContext jobCtx;
    private int lastHoldingResource; //resource(entity or dataset) which is held by this job lastly
    private int lastWaitingResource; //resource(entity or dataset) which this job is waiting for
    private int upgradingResource; //resource(entity or dataset) which this job is waiting for to upgrade

    public JobInfo(EntityInfoManager entityInfoManager, LockWaiterManager lockWaiterManager, TransactionContext txnCtx) {
        this.entityInfoManager = entityInfoManager;
        this.jobCtx = txnCtx;
        this.lastHoldingResource = -1;
        this.lastWaitingResource = -1;
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
    }
    
    public void addWaitingResource(int waiterObjId) {
        LockWaiter waiterObj;
        int entityInfo;
        
        if (lastWaitingResource != -1) {
            waiterObj = lockWaiterManager.getLockWaiter(lastHoldingResource);
            entityInfo = waiterObj.getEntityInfoSlot();
            entityInfoManager.setNextJobResource(entityInfo, waiterObjId);
        }
        
        waiterObj = lockWaiterManager.getLockWaiter(waiterObjId);
        entityInfo = waiterObj.getEntityInfoSlot();
        entityInfoManager.setPrevJobResource(entityInfo, lastWaitingResource);
        entityInfoManager.setNextJobResource(entityInfo, -1);
        lastWaitingResource = waiterObjId;
    }

    public void removeWaitingResource(int waiterObjId) {
        int currentId = lastWaitingResource;
        LockWaiter currentObj;
        int prevObjId;
        LockWaiter prevObj;
        int nextObjId;
        LockWaiter nextObj;
        int currentEntityInfo = 0;
        int prevEntityInfo = 0;
        int nextEntityInfo = 0;

        while (currentId != waiterObjId) {

            if (LockManager.IS_DEBUG_MODE) {
                if (currentId == -1) {
                    //shouldn't occur: debugging purpose
                    try {
                        throw new Exception();
                    } catch (Exception e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
            }

            currentObj = lockWaiterManager.getLockWaiter(currentId);
            currentEntityInfo = currentObj.getEntityInfoSlot();
            currentId = entityInfoManager.getPrevJobResource(currentEntityInfo);
        }

        currentObj = lockWaiterManager.getLockWaiter(currentId);
        currentEntityInfo = currentObj.getEntityInfoSlot();
        
        //getPrevJobResource through waiterObj
        prevObjId = entityInfoManager.getPrevJobResource(currentEntityInfo);
        if (prevObjId != -1) {
            prevObj = lockWaiterManager.getLockWaiter(prevObjId);
            prevEntityInfo = prevObj.getEntityInfoSlot();
        }
        
        //getNextJobResource through waiterObj
        nextObjId = entityInfoManager.getNextJobResource(currentEntityInfo);
        if (nextObjId != -1) {
            nextObj = lockWaiterManager.getLockWaiter(nextObjId);
            nextEntityInfo = nextObj.getEntityInfoSlot();
        }
        
        //update prev->next = next
        if (prevObjId != -1) {
            entityInfoManager.setNextJobResource(prevEntityInfo, nextObjId);
        }
        if (nextObjId != -1) {
            entityInfoManager.setPrevJobResource(nextEntityInfo, prevObjId);
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

    public void setLastWaitingResource(int resource) {
        lastWaitingResource = resource;
    }

    public int getLastWaitingResource() {
        return lastWaitingResource;
    }

    public void setUpgradingResource(int resource) {
        upgradingResource = resource;
    }

    public int getUpgradingResource() {
        return upgradingResource;
    }
}

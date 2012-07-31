package edu.uci.ics.asterix.transaction.management.service.locking;

import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionManagementConstants.LockManagerConstants.LockMode;

public class DatasetLockInfo {
    private EntityInfoManager entityInfoManager;
    private LockWaiterManager lockWaiterManager;
    private PrimitiveIntHashMap entityResourceHT;
    private int IXCount;
    private int ISCount;
    private int XCount;
    private int SCount;
    private int lastHolder;
    private int firstWaiter;
    private int firstUpgrader;

    public DatasetLockInfo(EntityInfoManager entityInfoManager, LockWaiterManager lockWaiterManager) {
        this.entityInfoManager = entityInfoManager;
        this.lockWaiterManager = lockWaiterManager; 
        entityResourceHT = new PrimitiveIntHashMap();
        lastHolder = -1; //-1 stands for end of list
        firstWaiter = -1;
        firstUpgrader = -1;
    }

    public void increaseLockCount(byte lockMode) {
        switch (lockMode) {
            case LockMode.IX:
                IXCount++;
                break;
            case LockMode.IS:
                ISCount++;
                break;
            case LockMode.X:
                XCount++;
                break;
            case LockMode.S:
                SCount++;
                break;
            default:
                throw new IllegalStateException("Invalid dataset lock mode");
        }
    }

    public void decreaseLockCount(byte lockMode) {
        switch (lockMode) {
            case LockMode.IX:
                IXCount--;
                break;
            case LockMode.IS:
                ISCount--;
                break;
            case LockMode.X:
                XCount--;
                break;
            case LockMode.S:
                SCount--;
                break;
            default:
                throw new IllegalStateException("Invalid dataset lock mode");
        }
    }

    public void increaseLockCount(byte lockMode, int count) {
        switch (lockMode) {
            case LockMode.IX:
                IXCount += count;
                break;
            case LockMode.IS:
                ISCount += count;
                break;
            case LockMode.X:
                XCount += count;
                break;
            case LockMode.S:
                SCount += count;
                break;
            default:
                throw new IllegalStateException("Invalid dataset lock mode");
        }
    }

    public void decreaseLockCount(byte lockMode, int count) {
        switch (lockMode) {
            case LockMode.IX:
                IXCount -= count;
                break;
            case LockMode.IS:
                ISCount -= count;
                break;
            case LockMode.X:
                XCount -= count;
                break;
            case LockMode.S:
                SCount -= count;
                break;
            default:
                throw new IllegalStateException("Invalid dataset lock mode");
        }
    }

    public boolean isUpgradeCompatible(byte lockMode, int entityInfo) {
        switch (lockMode) {
        //upgrade from IS -> IX
        //XCount is guaranteed to be 0.
        //upgrade is allowed if SCount is 0 except caller txn's count.
            case LockMode.IX:
                return SCount - entityInfoManager.getDatasetLockCount(entityInfo) == 0;

                //upgrade from S -> X
                //XCount and IXCount are guaranteed to be 0.
                //upgrade is allowed if ISCount is 0 except caller txn's count.
            case LockMode.X:
                return ISCount - entityInfoManager.getDatasetLockCount(entityInfo) == 0;

            default:
                throw new IllegalStateException("Invalid upgrade lock mode");
        }
    }

    public boolean isCompatible(byte lockMode) {
        switch (lockMode) {
            case LockMode.IX:
                return SCount == 0 && XCount == 0;

            case LockMode.IS:
                return XCount == 0;

            case LockMode.X:
                return ISCount == 0 && IXCount == 0 && SCount == 0 && XCount == 0;

            case LockMode.S:
                return IXCount == 0 && XCount == 0;

            default:
                throw new IllegalStateException("Invalid upgrade lock mode");
        }
    }

    public int findEntityInfoFromHolderList(int jobId, int hashVal) {
        int current;
        if (hashVal == -1) {//dataset-granule lock
            current = lastHolder;
            while (current != -1) {
                if (jobId == entityInfoManager.getJobId(current)) {
                    return current;
                }
                current = entityInfoManager.getPrevEntityActor(current);
            }
        } else { //entity-granule lock
            current = entityResourceHT.get(hashVal);
            while (current != -1) {
                if (jobId == entityInfoManager.getJobId(current)) {
                    return current;
                }
                current = entityInfoManager.getPrevEntityActor(current);
            }
        }
        return -1;
    }

    public boolean isNoHolder() {
        return ISCount == 0 && IXCount == 0 && SCount == 0 && XCount == 0;
    }

    public void addHolder(int holder) {
        entityInfoManager.setPrevEntityActor(holder, lastHolder);
        lastHolder = holder;
    }

    /**
     * Remove holder from linked list of Actor.
     * Also, remove the corresponding resource from linked list of resource
     * in order to minimize JobInfo's resource link traversal.
     * 
     * @param holder
     * @param jobInfo
     */
    public void removeHolder(int holder, JobInfo jobInfo) {
        int prev = lastHolder;
        int current = -1;
        int next;

        //remove holder from linked list of Actor
        while (prev != holder) {
            if (LockManager.IS_DEBUG_MODE) {
                if (prev == -1) {
                    //shouldn't occur: debugging purpose
                    try {
                        throw new Exception();
                    } catch (Exception e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
            }

            current = prev;
            prev = entityInfoManager.getPrevEntityActor(current);
        }

        if (current != -1) {
            //current->prev = prev->prev
            entityInfoManager.setPrevEntityActor(current, entityInfoManager.getPrevEntityActor(prev));
        } else {
            //lastHolder = prev->prev
            lastHolder = entityInfoManager.getPrevEntityActor(prev);
        }

        //Notice!!
        //remove the corresponding resource from linked list of resource.
        //it is guaranteed that there is no waiter or upgrader in the JobInfo when this function is called.
        prev = entityInfoManager.getPrevJobResource(holder);
        next = entityInfoManager.getNextJobResource(holder);

        if (prev != -1) {
            entityInfoManager.setNextJobResource(prev, next);
        }

        if (next != -1) {
            entityInfoManager.setPrevJobResource(next, prev);
        } else {
            //This entityInfo(i.e., holder) is the last resource held by this job.
            jobInfo.setlastHoldingResource(holder);
        }
    }

    /**
     * append new waiter to the end of waiters
     * @param waiterObjId
     */
    public void addWaiter(int waiterObjId) {
        int lastEntityInfo = 0;
        int lastObjId;
        LockWaiter lastObj;
        
        if (firstWaiter != -1) {
            //find the lastWaiter
            lastObjId = firstWaiter;
            while (lastObjId != -1) {
                lastObj = lockWaiterManager.getLockWaiter(lastObjId);
                lastEntityInfo = lastObj.getEntityInfoSlot();
                lastObjId = entityInfoManager.getNextEntityActor(lastEntityInfo);
            }
            //last->next = new_waiter
            entityInfoManager.setNextEntityActor(lastEntityInfo, waiterObjId);
        } else {
            firstWaiter = waiterObjId;
        }
        //new_waiter->next = -1
        lastObj = lockWaiterManager.getLockWaiter(waiterObjId);
        lastEntityInfo = lastObj.getEntityInfoSlot();
        entityInfoManager.setNextEntityActor(lastEntityInfo, -1);
    }

    public void removeWaiter(int waiterObjId) {
        int currentObjId = firstWaiter;
        LockWaiter currentObj;
        int currentEntityInfo = -1;
        LockWaiter prevObj;
        int prevEntityInfo = -1;
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
            prevEntityInfo = prevObj.getEntityInfoSlot();
            currentObjId = entityInfoManager.getNextEntityActor(prevEntityInfo);
        }

        //get current waiter object
        currentObj = lockWaiterManager.getLockWaiter(currentObjId);
        currentEntityInfo = currentObj.getEntityInfoSlot();
        
        //get next waiterObjId
        nextObjId = entityInfoManager.getNextEntityActor(currentEntityInfo);
        
        if (prevEntityInfo != -1) {
            //prev->next = next
            entityInfoManager.setNextEntityActor(prevEntityInfo, nextObjId);
        } else {
            //removed first waiter. firstWaiter = current->next
            firstWaiter = nextObjId;
        }
    }

    public void addUpgrader(int waiterObjId) {
        int lastEntityInfo = 0;
        int lastObjId;
        LockWaiter lastObj;
        
        if (firstWaiter != -1) {
            //find the lastWaiter
            lastObjId = firstWaiter;
            while (lastObjId != -1) {
                lastObj = lockWaiterManager.getLockWaiter(lastObjId);
                lastEntityInfo = lastObj.getEntityInfoSlot();
                lastObjId = entityInfoManager.getNextEntityActor(lastEntityInfo);
            }
            //last->next = new_waiter
            entityInfoManager.setNextEntityActor(lastEntityInfo, waiterObjId);
        } else {
            firstWaiter = waiterObjId;
        }
        //new_waiter->next = -1
        lastObj = lockWaiterManager.getLockWaiter(waiterObjId);
        lastEntityInfo = lastObj.getEntityInfoSlot();
        entityInfoManager.setNextEntityActor(lastEntityInfo, -1);
    }

    public void removeUpgrader(int waiterObjId) {
        int currentObjId = firstWaiter;
        LockWaiter currentObj;
        int currentEntityInfo = -1;
        LockWaiter prevObj;
        int prevEntityInfo = -1;
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
            prevEntityInfo = prevObj.getEntityInfoSlot();
            currentObjId = entityInfoManager.getNextEntityActor(prevEntityInfo);
        }

        //get current waiter object
        currentObj = lockWaiterManager.getLockWaiter(currentObjId);
        currentEntityInfo = currentObj.getEntityInfoSlot();
        
        //get next waiterObjId
        nextObjId = entityInfoManager.getNextEntityActor(currentEntityInfo);
        
        if (prevEntityInfo != -1) {
            //prev->next = next
            entityInfoManager.setNextEntityActor(prevEntityInfo, nextObjId);
        } else {
            //removed first waiter. firstWaiter = current->next
            firstWaiter = nextObjId;
        }
    }

    /**
     * wake up upgraders first, then waiters.
     */
    public void wakeUpWaiters() {
        boolean areAllUpgradersWakenUp = true;
        int waiterObjId = firstUpgrader;
        
        //wake up upgraders
        while (waiterObjId != -1) {
            
        }
    }

    /////////////////////////////////////////////////////////
    //  set/get method for private variable
    /////////////////////////////////////////////////////////
    public void setIXCount(int count) {
        IXCount = count;
    }

    public int getIXCount() {
        return IXCount;
    }

    public void setISCount(int count) {
        ISCount = count;
    }

    public int getISCount() {
        return ISCount;
    }

    public void setXCount(int count) {
        XCount = count;
    }

    public int getXCount() {
        return XCount;
    }

    public void setSCount(int count) {
        SCount = count;
    }

    public int getSCount() {
        return SCount;
    }

    public void setLastHolder(int holder) {
        lastHolder = holder;
    }

    public int getLastHolder() {
        return lastHolder;
    }

    public void setFirstWaiter(int waiter) {
        firstWaiter = waiter;
    }

    public int getFirstWaiter() {
        return firstWaiter;
    }

    public void setFirstUpgrader(int upgrader) {
        firstUpgrader = upgrader;
    }

    public int getFirstUpgrader() {
        return firstUpgrader;
    }

    public PrimitiveIntHashMap getEntityResourceHT() {
        return entityResourceHT;
    }


}
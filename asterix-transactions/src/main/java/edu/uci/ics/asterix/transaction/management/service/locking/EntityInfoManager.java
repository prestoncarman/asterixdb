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

import java.nio.ByteBuffer;
import java.util.ArrayList;

/**
 * EntityInfoManager provides EntityInfo arrays backed by ByteBuffer.
 * The array grows when the slots are overflowed.
 * Also, the array shrinks according to the following shrink policy
 * : Shrink when the resource under-utilization lasts for a certain threshold time.
 * 
 * @author kisskys
 */
public class EntityInfoManager {

    public static final int SHRINK_TIMER_THRESHOLD = 120000; //2min

    private ArrayList<ChildEntityInfoArrayManager> pArray;
    private int allocChild; //used to allocate the next free EntityInfo slot.
    private long shrinkTimer;
    private boolean isShrinkTimerOn;
    private int occupiedSlots;

    //    ////////////////////////////////////////////////
    //    // begin of unit test
    //    ////////////////////////////////////////////////
    //
    //    public static final int SHRINK_TIMER_THRESHOLD = 0; //for unit test
    //
    //    /**
    //     * @param args
    //     */
    //    public static void main(String[] args) {
    //        final int DataSize = 5000;
    //
    //        int i, j;
    //        int slots = ChildEntityInfoArrayManager.NUM_OF_SLOTS;
    //        int data[] = new int[DataSize];
    //        EntityInfoManager eiMgr = new EntityInfoManager();
    //
    //        //allocate: 50
    //        System.out.println("allocate: 50");
    //        for (i = 0; i < 5; i++) {
    //            for (j = i * slots; j < i * slots + slots; j++) {
    //                data[j] = eiMgr.allocate();
    //            }
    //
    //            System.out.println(eiMgr.prettyPrint());
    //        }
    //
    //        //deallocate from the last child to the first child
    //        System.out.println("deallocate from the last child to the first child");
    //        for (i = 4; i >= 0; i--) {
    //            for (j = i * slots + slots - 1; j >= i * slots; j--) {
    //                eiMgr.deallocate(data[j]);
    //            }
    //            System.out.println(eiMgr.prettyPrint());
    //        }
    //
    //        //allocate: 50
    //        System.out.println("allocate: 50");
    //        for (i = 0; i < 5; i++) {
    //            for (j = i * slots; j < i * slots + slots; j++) {
    //                data[j] = eiMgr.allocate();
    //            }
    //
    //            System.out.println(eiMgr.prettyPrint());
    //        }
    //
    //        //deallocate from the first child to last child
    //        System.out.println("deallocate from the first child to last child");
    //        for (i = 0; i < 5; i++) {
    //            for (j = i * slots; j < i * slots + slots; j++) {
    //                eiMgr.deallocate(data[j]);
    //            }
    //
    //            System.out.println(eiMgr.prettyPrint());
    //        }
    //
    //        //allocate: 50
    //        System.out.println("allocate: 50");
    //        for (i = 0; i < 5; i++) {
    //            for (j = i * slots; j < i * slots + slots; j++) {
    //                data[j] = eiMgr.allocate();
    //            }
    //
    //            System.out.println(eiMgr.prettyPrint());
    //        }
    //
    //        //deallocate from the first child to 4th child
    //        System.out.println("deallocate from the first child to 4th child");
    //        for (i = 0; i < 4; i++) {
    //            for (j = i * slots; j < i * slots + slots; j++) {
    //                eiMgr.deallocate(data[j]);
    //            }
    //
    //            System.out.println(eiMgr.prettyPrint());
    //        }
    //
    //        //allocate: 40
    //        System.out.println("allocate: 40");
    //        for (i = 0; i < 4; i++) {
    //            for (j = i * slots; j < i * slots + slots; j++) {
    //                data[j] = eiMgr.allocate();
    //            }
    //
    //            System.out.println(eiMgr.prettyPrint());
    //        }
    //    }
    //    
    //    ////////////////////////////////////////////////
    //    // end of unit test
    //    ////////////////////////////////////////////////

    public EntityInfoManager() {
        pArray = new ArrayList<ChildEntityInfoArrayManager>();
        pArray.add(new ChildEntityInfoArrayManager());
        allocChild = 0;
        occupiedSlots = 0;
        isShrinkTimerOn = false;
    }

    public int allocate() {
        if (pArray.get(allocChild).isFull()) {
            int size = pArray.size();
            boolean bAlloc = false;
            ChildEntityInfoArrayManager child;

            //find a deinitialized child and initialze it
            for (int i = 0; i < size; i++) {
                child = pArray.get(i);
                if (child.isDeinitialized()) {
                    child.initialize();
                    allocChild = i;
                    bAlloc = true;
                    break;
                }
            }

            //allocate new child when there is no deinitialized child
            if (!bAlloc) {
                pArray.add(new ChildEntityInfoArrayManager());
                allocChild = pArray.size() - 1;
            }
        }
        occupiedSlots++;
        return pArray.get(allocChild).allocate() + allocChild * ChildEntityInfoArrayManager.NUM_OF_SLOTS;
    }

    void deallocate(int slotNum) {
        pArray.get(slotNum / ChildEntityInfoArrayManager.NUM_OF_SLOTS).deallocate(
                slotNum % ChildEntityInfoArrayManager.NUM_OF_SLOTS);
        occupiedSlots--;

        if (needShrink()) {
            shrink();
        }
    }

    /**
     * Shrink policy:
     * Shrink when the resource under-utilization lasts for a certain amount of time.
     * TODO Need to figure out which of the policies is better
     * case1.
     * pArray status : O x x x x x O (O is initialized, x is deinitialized)
     * In the above status, 'CURRENT' needShrink() returns 'TRUE'
     * even if there is nothing to shrink or deallocate.
     * It doesn't distinguish the deinitialized children from initialized children
     * by calculating totalNumOfSlots = pArray.size() * ChildEntityInfoArrayManager.NUM_OF_SLOTS.
     * In other words, it doesn't subtract the deinitialized children's slots.
     * case2.
     * pArray status : O O x x x x x
     * However, in the above case, if we subtract the deinitialized children's slots,
     * needShrink() will return false even if we shrink the pArray at this case.
     * 
     * @return
     */
    private boolean needShrink() {
        int size = pArray.size();
        int usedSlots = occupiedSlots;
        if (usedSlots == 0) {
            usedSlots = 1;
        }

        if (size > 1 && size * ChildEntityInfoArrayManager.NUM_OF_SLOTS / usedSlots >= 3) {
            if (isShrinkTimerOn) {
                if (System.currentTimeMillis() - shrinkTimer >= SHRINK_TIMER_THRESHOLD) {
                    isShrinkTimerOn = false;
                    return true;
                }
            } else {
                //turn on timer
                isShrinkTimerOn = true;
                shrinkTimer = System.currentTimeMillis();
            }
        } else {
            //turn off timer
            isShrinkTimerOn = false;
        }

        return false;
    }

    /**
     * Shrink() may
     * deinitialize(:deallocates ByteBuffer of child) Children(s) or
     * shrink pArray according to the deinitialized children's contiguity status.
     * It doesn't deinitialze or shrink more than half of children at a time.
     */
    private void shrink() {
        int i;
        boolean bContiguous = true;
        int decreaseCount = 0;
        int size = pArray.size();
        int maxDecreaseCount = size / 2;
        ChildEntityInfoArrayManager child;
        for (i = size - 1; i >= 0; i--) {
            child = pArray.get(i);
            if (child.isEmpty() || child.isDeinitialized()) {
                if (bContiguous) {
                    pArray.remove(i);
                    if (++decreaseCount == maxDecreaseCount) {
                        break;
                    }
                } else {
                    bContiguous = false;
                    if (child.isEmpty()) {
                        child.deinitialize();
                        if (++decreaseCount == maxDecreaseCount) {
                            break;
                        }
                    }
                }
            } else {
                bContiguous = false;
            }
        }

        //reset allocChild when the child is removed or deinitialized.
        size = pArray.size();
        if (allocChild >= size || pArray.get(allocChild).isDeinitialized()) {
            //set allocChild to any initialized one.
            //It is guaranteed that there is at least one initialized child.
            for (i = 0; i < size; i++) {
                if (!pArray.get(i).isDeinitialized()) {
                    allocChild = i;
                    break;
                }
            }
        }
    }

    public String prettyPrint() {
        StringBuilder s = new StringBuilder("\n########### EntityInfoManager Status #############\n");
        int size = pArray.size();
        ChildEntityInfoArrayManager child;

        for (int i = 0; i < size; i++) {
            child = pArray.get(i);
            if (child.isDeinitialized()) {
                continue;
            }
            s.append("child[" + i + "]: occupiedSlots:" + child.getNumOfOccupiedSlots());
            s.append(" freeSlotNum:" + child.getFreeSlotNum() + "\n");
            for (int j = 0; j < ChildEntityInfoArrayManager.NUM_OF_SLOTS; j++) {
                s.append(j).append(": ");
                s.append("\t" + child.getJobId(j));
                s.append("\t" + child.getDatasetId(j));
                s.append("\t" + child.getPKHashVal(j));
                s.append("\t" + child.getLockMode(j));
                s.append("\t" + child.getLockCount(j));
                s.append("\t" + child.getNextDatasetActor(j));
                s.append("\t" + child.getNextEntityActor(j));
                s.append("\t" + child.getPrevJobResource(j));
                s.append("\n");
            }
            s.append("\n");
        }
        return s.toString();
    }

    //////////////////////////////////////////////////////////////////
    //   set/get method for each field of EntityInfo
    //////////////////////////////////////////////////////////////////

    public void setJobId(int slotNum, int id) {
        pArray.get(slotNum / ChildEntityInfoArrayManager.NUM_OF_SLOTS).setJobId(
                slotNum % ChildEntityInfoArrayManager.NUM_OF_SLOTS, id);
    }

    public int getJobId(int slotNum) {
        return pArray.get(slotNum / ChildEntityInfoArrayManager.NUM_OF_SLOTS).getJobId(
                slotNum % ChildEntityInfoArrayManager.NUM_OF_SLOTS);
    }

    public void setDatasetId(int slotNum, int id) {
        pArray.get(slotNum / ChildEntityInfoArrayManager.NUM_OF_SLOTS).setDatasetId(
                slotNum % ChildEntityInfoArrayManager.NUM_OF_SLOTS, id);
    }

    public int getDatasetId(int slotNum) {
        return pArray.get(slotNum / ChildEntityInfoArrayManager.NUM_OF_SLOTS).getDatasetId(
                slotNum % ChildEntityInfoArrayManager.NUM_OF_SLOTS);
    }

    public void setPKHashVal(int slotNum, int hashVal) {
        pArray.get(slotNum / ChildEntityInfoArrayManager.NUM_OF_SLOTS).setPKHashVal(
                slotNum % ChildEntityInfoArrayManager.NUM_OF_SLOTS, hashVal);
    }

    public int getPKHashVal(int slotNum) {
        return pArray.get(slotNum / ChildEntityInfoArrayManager.NUM_OF_SLOTS).getPKHashVal(
                slotNum % ChildEntityInfoArrayManager.NUM_OF_SLOTS);
    }

    public void setLockMode(int slotNum, byte mode) {
        pArray.get(slotNum / ChildEntityInfoArrayManager.NUM_OF_SLOTS).setLockMode(
                slotNum % ChildEntityInfoArrayManager.NUM_OF_SLOTS, mode);
    }

    public byte getLockMode(int slotNum) {
        return pArray.get(slotNum / ChildEntityInfoArrayManager.NUM_OF_SLOTS).getLockMode(
                slotNum % ChildEntityInfoArrayManager.NUM_OF_SLOTS);
    }

    public void setLockCount(int slotNum, byte count) {
        pArray.get(slotNum / ChildEntityInfoArrayManager.NUM_OF_SLOTS).setLockCount(
                slotNum % ChildEntityInfoArrayManager.NUM_OF_SLOTS, count);
    }

    public byte getLockCount(int slotNum) {
        return pArray.get(slotNum / ChildEntityInfoArrayManager.NUM_OF_SLOTS).getLockCount(
                slotNum % ChildEntityInfoArrayManager.NUM_OF_SLOTS);
    }

    public void setNextDatasetActor(int slotNum, int nextActorSlotNum) {
        pArray.get(slotNum / ChildEntityInfoArrayManager.NUM_OF_SLOTS).setNextDatasetActor(
                slotNum % ChildEntityInfoArrayManager.NUM_OF_SLOTS, nextActorSlotNum);
    }

    public int getNextDatasetActor(int slotNum) {
        return pArray.get(slotNum / ChildEntityInfoArrayManager.NUM_OF_SLOTS).getNextDatasetActor(
                slotNum % ChildEntityInfoArrayManager.NUM_OF_SLOTS);
    }

    public void setNextEntityActor(int slotNum, int nextActorSlotNum) {
        pArray.get(slotNum / ChildEntityInfoArrayManager.NUM_OF_SLOTS).setNextEntityActor(
                slotNum % ChildEntityInfoArrayManager.NUM_OF_SLOTS, nextActorSlotNum);
    }

    public int getNextEntityActor(int slotNum) {
        return pArray.get(slotNum / ChildEntityInfoArrayManager.NUM_OF_SLOTS).getNextEntityActor(
                slotNum % ChildEntityInfoArrayManager.NUM_OF_SLOTS);
    }

    public void setPrevJobResource(int slotNum, int prevResourceSlotNum) {
        pArray.get(slotNum / ChildEntityInfoArrayManager.NUM_OF_SLOTS).setPrevJobResource(
                slotNum % ChildEntityInfoArrayManager.NUM_OF_SLOTS, prevResourceSlotNum);
    }

    public int getPrevJobResource(int slotNum) {
        return pArray.get(slotNum / ChildEntityInfoArrayManager.NUM_OF_SLOTS).getPrevJobResource(
                slotNum % ChildEntityInfoArrayManager.NUM_OF_SLOTS);
    }
}

/******************************************
 * EntityInfo (26 bytes)
 * ****************************************
 * int jobId
 * int datasetId
 * int PKHashValue
 * byte lockMode
 * byte lockCount
 * int nextDatasetActor : actor can be either holder/waiter/upgrader
 * int nextEntityActor : actor can be either holder/waiter/upgrader
 * int prevJobResource : resource can be either dataset or entity and a job is holding/waiting/upgrading lock(s) on it.
 *******************************************/

class ChildEntityInfoArrayManager {
    public static final int ENTITY_INFO_SIZE = 26; //26bytes
    public static final int NUM_OF_SLOTS = 1024; //number of entities in a buffer
    //    public static final int NUM_OF_SLOTS = 10; //for unit test
    public static final int BUFFER_SIZE = ENTITY_INFO_SIZE * NUM_OF_SLOTS;

    //byte offset of each field of EntityInfo
    public static final int JOB_ID_OFFSET = 0;
    public static final int DATASET_ID_OFFSET = 4;
    public static final int PKHASH_VAL_OFFSET = 8;
    public static final int LOCK_MODE_OFFSET = 12;
    public static final int LOCK_COUNT_OFFSET = 13;
    public static final int DATASET_ACTOR_OFFSET = 14;
    public static final int ENTITY_ACTOR_OFFSET = 18;
    public static final int JOB_RESOURCE_OFFSET = 22;

    //byte offset of nextFreeSlotNum which shares the same space of JobId
    //If a slot is in use, the space is used for JobId. Otherwise, it is used for nextFreeSlotNum. 
    public static final int NEXT_FREE_SLOT_OFFSET = 0;

    private ByteBuffer buffer;
    private int freeSlotNum;
    private int occupiedSlots; //-1 represents 'deinitialized' state.

    public ChildEntityInfoArrayManager() {
        initialize();
    }

    public void initialize() {
        this.buffer = ByteBuffer.allocate(BUFFER_SIZE);
        this.freeSlotNum = 0;
        this.occupiedSlots = 0;

        for (int i = 0; i < NUM_OF_SLOTS - 1; i++) {
            setNextFreeSlot(i, i + 1);
        }
        setNextFreeSlot(NUM_OF_SLOTS - 1, -1); //-1 represents EOL(end of link)
    }

    public int allocate() {
        int currentSlot = freeSlotNum;
        freeSlotNum = getNextFreeSlot(currentSlot);
        occupiedSlots++;
        return currentSlot;
    }

    public void deallocate(int slotNum) {
        setNextFreeSlot(slotNum, freeSlotNum);
        freeSlotNum = slotNum;
        occupiedSlots--;
    }

    public void deinitialize() {
        buffer = null;
        occupiedSlots = -1;
    }

    public boolean isDeinitialized() {
        return occupiedSlots == -1;
    }

    public boolean isFull() {
        return occupiedSlots == NUM_OF_SLOTS;
    }

    public boolean isEmpty() {
        return occupiedSlots == 0;
    }

    public int getNumOfOccupiedSlots() {
        return occupiedSlots;
    }

    public int getFreeSlotNum() {
        return freeSlotNum;
    }

    //////////////////////////////////////////////////////////////////
    //   set/get method for each field of EntityInfo plus freeSlot
    //////////////////////////////////////////////////////////////////

    public void setNextFreeSlot(int slotNum, int nextFreeSlot) {
        buffer.putInt(slotNum * ENTITY_INFO_SIZE + NEXT_FREE_SLOT_OFFSET, nextFreeSlot);
    }

    public int getNextFreeSlot(int slotNum) {
        return buffer.getInt(slotNum * ENTITY_INFO_SIZE + NEXT_FREE_SLOT_OFFSET);
    }

    public void setJobId(int slotNum, int id) {
        buffer.putInt(slotNum * ENTITY_INFO_SIZE + JOB_ID_OFFSET, id);
    }

    public int getJobId(int slotNum) {
        return buffer.getInt(slotNum * ENTITY_INFO_SIZE + JOB_ID_OFFSET);
    }

    public void setDatasetId(int slotNum, int id) {
        buffer.putInt(slotNum * ENTITY_INFO_SIZE + DATASET_ID_OFFSET, id);
    }

    public int getDatasetId(int slotNum) {
        return buffer.getInt(slotNum * ENTITY_INFO_SIZE + DATASET_ID_OFFSET);
    }

    public void setPKHashVal(int slotNum, int hashVal) {
        buffer.putInt(slotNum * ENTITY_INFO_SIZE + PKHASH_VAL_OFFSET, hashVal);
    }

    public int getPKHashVal(int slotNum) {
        return buffer.getInt(slotNum * ENTITY_INFO_SIZE + PKHASH_VAL_OFFSET);
    }

    public void setLockMode(int slotNum, byte mode) {
        buffer.put(slotNum * ENTITY_INFO_SIZE + LOCK_MODE_OFFSET, mode);
    }

    public byte getLockMode(int slotNum) {
        return buffer.get(slotNum * ENTITY_INFO_SIZE + LOCK_MODE_OFFSET);
    }

    public void setLockCount(int slotNum, byte count) {
        buffer.put(slotNum * ENTITY_INFO_SIZE + LOCK_COUNT_OFFSET, count);
    }

    public byte getLockCount(int slotNum) {
        return buffer.get(slotNum * ENTITY_INFO_SIZE + LOCK_COUNT_OFFSET);
    }

    public void setNextDatasetActor(int slotNum, int nextActorSlotNum) {
        buffer.putInt(slotNum * ENTITY_INFO_SIZE + DATASET_ACTOR_OFFSET, nextActorSlotNum);
    }

    public int getNextDatasetActor(int slotNum) {
        return buffer.getInt(slotNum * ENTITY_INFO_SIZE + DATASET_ACTOR_OFFSET);
    }

    public void setNextEntityActor(int slotNum, int nextActorSlotNum) {
        buffer.putInt(slotNum * ENTITY_INFO_SIZE + ENTITY_ACTOR_OFFSET, nextActorSlotNum);
    }

    public int getNextEntityActor(int slotNum) {
        return buffer.getInt(slotNum * ENTITY_INFO_SIZE + ENTITY_ACTOR_OFFSET);
    }

    public void setPrevJobResource(int slotNum, int prevResourceSlotNum) {
        buffer.putInt(slotNum * ENTITY_INFO_SIZE + JOB_RESOURCE_OFFSET, prevResourceSlotNum);
    }

    public int getPrevJobResource(int slotNum) {
        return buffer.getInt(slotNum * ENTITY_INFO_SIZE + JOB_RESOURCE_OFFSET);
    }
}
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
 * EntityLockInfoManager provides EntityLockInfo arrays backed by ByteBuffer.
 * The array grows when the slots are overflowed.
 * Also, the array shrinks according to the following shrink policy
 * : Shrink when the resource under-utilization lasts for a certain threshold time.
 * 
 * @author kisskys
 */
public class EntityLockInfoManager {

    public static final int SHRINK_TIMER_THRESHOLD = 120000; //2min

    private ArrayList<ChildEntityLockInfoArrayManager> pArray;
    private int allocChild; //used to allocate the next free EntityInfo slot.
    private long shrinkTimer;
    private boolean isShrinkTimerOn;
    private int occupiedSlots;
    private EntityInfoManager entityInfoManager;

//        ////////////////////////////////////////////////
//        // begin of unit test
//        ////////////////////////////////////////////////
//    
//        public static final int SHRINK_TIMER_THRESHOLD = 0; //for unit test
//    
//        /**
//         * @param args
//         */
//        public static void main(String[] args) {
//            final int DataSize = 5000;
//    
//            int i, j;
//            int slots = ChildEntityLockInfoArrayManager.NUM_OF_SLOTS;
//            int data[] = new int[DataSize];
//            EntityLockInfoManager eliMgr = new EntityLockInfoManager();
//    
//            //allocate: 50
//            System.out.println("allocate: 50");
//            for (i = 0; i < 5; i++) {
//                for (j = i * slots; j < i * slots + slots; j++) {
//                    data[j] = eliMgr.allocate();
//                }
//    
//                System.out.println(eliMgr.prettyPrint());
//            }
//    
//            //deallocate from the last child to the first child
//            System.out.println("deallocate from the last child to the first child");
//            for (i = 4; i >= 0; i--) {
//                for (j = i * slots + slots - 1; j >= i * slots; j--) {
//                    eliMgr.deallocate(data[j]);
//                }
//                System.out.println(eliMgr.prettyPrint());
//            }
//    
//            //allocate: 50
//            System.out.println("allocate: 50");
//            for (i = 0; i < 5; i++) {
//                for (j = i * slots; j < i * slots + slots; j++) {
//                    data[j] = eliMgr.allocate();
//                }
//    
//                System.out.println(eliMgr.prettyPrint());
//            }
//    
//            //deallocate from the first child to last child
//            System.out.println("deallocate from the first child to last child");
//            for (i = 0; i < 5; i++) {
//                for (j = i * slots; j < i * slots + slots; j++) {
//                    eliMgr.deallocate(data[j]);
//                }
//    
//                System.out.println(eliMgr.prettyPrint());
//            }
//    
//            //allocate: 50
//            System.out.println("allocate: 50");
//            for (i = 0; i < 5; i++) {
//                for (j = i * slots; j < i * slots + slots; j++) {
//                    data[j] = eliMgr.allocate();
//                }
//    
//                System.out.println(eliMgr.prettyPrint());
//            }
//    
//            //deallocate from the first child to 4th child
//            System.out.println("deallocate from the first child to 4th child");
//            for (i = 0; i < 4; i++) {
//                for (j = i * slots; j < i * slots + slots; j++) {
//                    eliMgr.deallocate(data[j]);
//                }
//    
//                System.out.println(eliMgr.prettyPrint());
//            }
//    
//            //allocate: 40
//            System.out.println("allocate: 40");
//            for (i = 0; i < 4; i++) {
//                for (j = i * slots; j < i * slots + slots; j++) {
//                    data[j] = eliMgr.allocate();
//                }
//    
//                System.out.println(eliMgr.prettyPrint());
//            }
//        }
//        
//        ////////////////////////////////////////////////
//        // end of unit test
//        ////////////////////////////////////////////////

    public EntityLockInfoManager(EntityInfoManager entityInfoManager) {
        pArray = new ArrayList<ChildEntityLockInfoArrayManager>();
        pArray.add(new ChildEntityLockInfoArrayManager());
        allocChild = 0;
        occupiedSlots = 0;
        isShrinkTimerOn = false;
        this.entityInfoManager = entityInfoManager;
    }

    public int allocate() {
        if (pArray.get(allocChild).isFull()) {
            int size = pArray.size();
            boolean bAlloc = false;
            ChildEntityLockInfoArrayManager child;

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
                pArray.add(new ChildEntityLockInfoArrayManager());
                allocChild = pArray.size() - 1;
            }
        }
        occupiedSlots++;
        return pArray.get(allocChild).allocate() + allocChild * ChildEntityLockInfoArrayManager.NUM_OF_SLOTS;
    }

    void deallocate(int slotNum) {
        pArray.get(slotNum / ChildEntityLockInfoArrayManager.NUM_OF_SLOTS).deallocate(
                slotNum % ChildEntityLockInfoArrayManager.NUM_OF_SLOTS);
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
     * by calculating totalNumOfSlots = pArray.size() * ChildEntityLockInfoArrayManager.NUM_OF_SLOTS.
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

        if (size > 1 && size * ChildEntityLockInfoArrayManager.NUM_OF_SLOTS / usedSlots >= 3) {
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
        ChildEntityLockInfoArrayManager child;
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
        StringBuilder s = new StringBuilder("\n########### EntityLockInfoManager Status #############\n");
        int size = pArray.size();
        ChildEntityLockInfoArrayManager child;

        for (int i = 0; i < size; i++) {
            child = pArray.get(i);
            if (child.isDeinitialized()) {
                continue;
            }
            s.append("child[" + i + "]: occupiedSlots:" + child.getNumOfOccupiedSlots());
            s.append(" freeSlotNum:" + child.getFreeSlotNum() + "\n");
            for (int j = 0; j < ChildEntityLockInfoArrayManager.NUM_OF_SLOTS; j++) {
                s.append(j).append(": ");
                s.append("\t" + child.getXCount(j));
                s.append("\t" + child.getSCount(j));
                s.append("\t" + child.getLastHolder(j));
                s.append("\t" + child.getFirstWaiter(j));
                s.append("\t" + child.getUpgrader(j));
                s.append("\n");
            }
            s.append("\n");
        }
        return s.toString();
    }
    
    //////////////////////////////////////////////////////////////////
    //   set/get method for each field of EntityLockInfo
    //////////////////////////////////////////////////////////////////

    public void setXCount(int slotNum, int count) {
        pArray.get(slotNum / ChildEntityInfoArrayManager.NUM_OF_SLOTS).setXCount(
                slotNum % ChildEntityInfoArrayManager.NUM_OF_SLOTS, count);
    }

    public int getXCount(int slotNum) {
        return pArray.get(slotNum / ChildEntityInfoArrayManager.NUM_OF_SLOTS).getXCount(
                slotNum % ChildEntityInfoArrayManager.NUM_OF_SLOTS);
    }

    public void setSCount(int slotNum, int count) {
        pArray.get(slotNum / ChildEntityInfoArrayManager.NUM_OF_SLOTS).setSCount(
                slotNum % ChildEntityInfoArrayManager.NUM_OF_SLOTS, count);
    }

    public int getSCount(int slotNum) {
        return pArray.get(slotNum / ChildEntityInfoArrayManager.NUM_OF_SLOTS).getSCount(
                slotNum % ChildEntityInfoArrayManager.NUM_OF_SLOTS);
    }

    public void setLastHolder(int slotNum, int holder) {
        pArray.get(slotNum / ChildEntityInfoArrayManager.NUM_OF_SLOTS).setLastHolder(
                slotNum % ChildEntityInfoArrayManager.NUM_OF_SLOTS, holder);
    }

    public int getLastHolder(int slotNum) {
        return pArray.get(slotNum / ChildEntityInfoArrayManager.NUM_OF_SLOTS).getLastHolder(
                slotNum % ChildEntityInfoArrayManager.NUM_OF_SLOTS);
    }

    public void setFirstWaiter(int slotNum, int waiter) {
        pArray.get(slotNum / ChildEntityInfoArrayManager.NUM_OF_SLOTS).setFirstWaiter(
                slotNum % ChildEntityInfoArrayManager.NUM_OF_SLOTS, waiter);
    }

    public int getFirstWaiter(int slotNum) {
        return pArray.get(slotNum / ChildEntityInfoArrayManager.NUM_OF_SLOTS).getFirstWaiter(
                slotNum % ChildEntityInfoArrayManager.NUM_OF_SLOTS);
    }

    public void setUpgrader(int slotNum, int upgrader) {
        pArray.get(slotNum / ChildEntityInfoArrayManager.NUM_OF_SLOTS).setUpgrader(
                slotNum % ChildEntityInfoArrayManager.NUM_OF_SLOTS, upgrader);
    }

    public byte getUpgrader(int slotNum) {
        return pArray.get(slotNum / ChildEntityInfoArrayManager.NUM_OF_SLOTS).getUpgrader(
                slotNum % ChildEntityInfoArrayManager.NUM_OF_SLOTS);
    }
}

/******************************************
 * EntityLockInfo (20 bytes)
 * ****************************************
 * int XCount : used to represent the count of X mode lock if it is allocated. Otherwise, it represents next free slot.
 * int SCount
 * int firstHolder
 * int firstWaiter
 * int upgrader : may exist only one since there are only S and X mode lock in Entity-level
 *******************************************/

class ChildEntityLockInfoArrayManager {
    public static final int ENTITY_LOCK_INFO_SIZE = 20; //20bytes
    public static final int NUM_OF_SLOTS = 1024; //number of entityLockInfos in a buffer
    //public static final int NUM_OF_SLOTS = 10; //for unit test
    public static final int BUFFER_SIZE = ENTITY_LOCK_INFO_SIZE * NUM_OF_SLOTS;

    //byte offset of each field of EntityLockInfo
    public static final int XCOUNT_OFFSET = 0;
    public static final int SCOUNT_OFFSET = 4;
    public static final int LAST_HOLDER_OFFSET = 8;
    public static final int FIRST_WAITER_OFFSET = 12;
    public static final int UPGRADER_OFFSET = 16;

    //byte offset of nextFreeSlotNum which shares the same space with XCount field
    //If a slot is in use, the space is used for XCount. Otherwise, it is used for nextFreeSlotNum. 
    public static final int NEXT_FREE_SLOT_OFFSET = 0;

    private ByteBuffer buffer;
    private int freeSlotNum;
    private int occupiedSlots; //-1 represents 'deinitialized' state.

    public ChildEntityLockInfoArrayManager() {
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
    //   set/get method for each field of EntityLockInfo plus freeSlot
    //////////////////////////////////////////////////////////////////

    public void setNextFreeSlot(int slotNum, int nextFreeSlot) {
        buffer.putInt(slotNum * ENTITY_LOCK_INFO_SIZE + NEXT_FREE_SLOT_OFFSET, nextFreeSlot);
    }

    public int getNextFreeSlot(int slotNum) {
        return buffer.getInt(slotNum * ENTITY_LOCK_INFO_SIZE + NEXT_FREE_SLOT_OFFSET);
    }

    public void setXCount(int slotNum, int count) {
        buffer.putInt(slotNum * ENTITY_LOCK_INFO_SIZE + XCOUNT_OFFSET, count);
    }

    public int getXCount(int slotNum) {
        return buffer.getInt(slotNum * ENTITY_LOCK_INFO_SIZE + XCOUNT_OFFSET);
    }

    public void setSCount(int slotNum, int count) {
        buffer.putInt(slotNum * ENTITY_LOCK_INFO_SIZE + SCOUNT_OFFSET, count);
    }

    public int getSCount(int slotNum) {
        return buffer.getInt(slotNum * ENTITY_LOCK_INFO_SIZE + SCOUNT_OFFSET);
    }

    public void setLastHolder(int slotNum, int holder) {
        buffer.putInt(slotNum * ENTITY_LOCK_INFO_SIZE + LAST_HOLDER_OFFSET, holder);
    }

    public int getLastHolder(int slotNum) {
        return buffer.getInt(slotNum * ENTITY_LOCK_INFO_SIZE + LAST_HOLDER_OFFSET);
    }

    public void setFirstWaiter(int slotNum, int waiter) {
        buffer.putInt(slotNum * ENTITY_LOCK_INFO_SIZE + FIRST_WAITER_OFFSET, waiter);
    }

    public int getFirstWaiter(int slotNum) {
        return buffer.get(slotNum * ENTITY_LOCK_INFO_SIZE + FIRST_WAITER_OFFSET);
    }

    public void setUpgrader(int slotNum, int upgrader) {
        buffer.putInt(slotNum * ENTITY_LOCK_INFO_SIZE + UPGRADER_OFFSET, upgrader);
    }

    public byte getUpgrader(int slotNum) {
        return buffer.get(slotNum * ENTITY_LOCK_INFO_SIZE + UPGRADER_OFFSET);
    }
}
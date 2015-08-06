/*
 * Copyright 2009-2013 by The Regents of the University of California
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
package edu.uci.ics.asterix.runtime.operators.interval;

public class SortMergeIntervalStatus {
    public enum BranchStatus {
        UNKNOWN,
        OPENED,
        DATA_PROCESSING,
        CLOSED,
        FAILED,
    }

    public enum RunFileStatus {
        NOT_USED,
        WRITING,
        READING,
    }

    public boolean reloadingLeftFrame = false;
    public boolean loadRightFrame = false;

    public boolean leftHasMore = true;
    public boolean rightHasMore = true;

    private BranchStatus leftStatus = BranchStatus.UNKNOWN;
    private BranchStatus rightStatus = BranchStatus.UNKNOWN;

    public RunFileStatus runFileStatus = RunFileStatus.NOT_USED;

    public SortMergeIntervalStatus() {
    }

    public BranchStatus getLeftStatus() {
        return leftStatus;
    }

    public BranchStatus getRightStatus() {
        return rightStatus;
    }

    public void openLeft() {
        leftStatus = BranchStatus.OPENED;
    }

    public void openRight() {
        rightStatus = BranchStatus.OPENED;
    }

    public void dataLeft() {
        leftStatus = BranchStatus.DATA_PROCESSING;
    }

    public void dataRight() {
        rightStatus = BranchStatus.DATA_PROCESSING;
    }

    public void closeLeft() {
        leftStatus = BranchStatus.CLOSED;
    }

    public void closeRight() {
        rightStatus = BranchStatus.CLOSED;
    }

}

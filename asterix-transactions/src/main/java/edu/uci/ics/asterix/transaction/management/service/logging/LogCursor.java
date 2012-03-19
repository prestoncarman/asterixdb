/*
 * Copyright 2009-2010 by The Regents of the University of California
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
package edu.uci.ics.asterix.transaction.management.service.logging;

import java.io.File;
import java.io.IOException;

import edu.uci.ics.asterix.transaction.management.exception.ACIDException;

public class LogCursor implements ILogCursor {

    private final ILogManager logManager;
    private final ILogFilter logFilter;
    private IFileBasedBuffer readOnlyBuffer;
    private LogicalLogLocator nextLogicalLogLocator = null;
    private int bufferIndex = 0;

    /**
     * @param logFilter
     */
    public LogCursor(final ILogManager logManager, ILogFilter logFilter) throws ACIDException {
        this.logFilter = logFilter;
        this.logManager = logManager;

    }

    public LogCursor(final ILogManager logManager, PhysicalLogLocator startingPhysicalLogLocator, ILogFilter logFilter)
            throws IOException {
        this.logFilter = logFilter;
        this.logManager = logManager;
        initialize(startingPhysicalLogLocator);
    }

    private void initialize(final PhysicalLogLocator startingPhysicalLogLocator) throws IOException {
        readOnlyBuffer = getReadOnlyBuffer(startingPhysicalLogLocator.getLsn(), logManager.getLogManagerProperties()
                .getLogBufferSize());
        nextLogicalLogLocator = new LogicalLogLocator(startingPhysicalLogLocator.getLsn(), readOnlyBuffer, 0, logManager);
    }

    private IFileBasedBuffer getReadOnlyBuffer(long lsn, int size) throws IOException {
        int fileId = (int) (lsn / logManager.getLogManagerProperties().getLogPartitionSize());
        String filePath = LogUtil.getLogFilePath(logManager.getLogManagerProperties(), fileId);
        File file = new File(filePath);
        if (file.exists()) {
            return FileUtil.getFileBasedBuffer(filePath, lsn, size);
        } else {
            return null;
        }
    }

    /**
     * Moves the cursor to the next log record. 
     * The parameter, logLocator is set to the point to the next log record.
     * The usage of this method is as follows.
     * When next() is called for the first time, the cursor is set to the log record of which LSN is the LSN 
     * specified by the LogCursor constructor and the parameter, logLocator indicates the same log record.
     * [Notice] The log cursor user has to know the first valid log record's LSN.
     * Now, Whenever next() is called, the cursor and the logLocator moves to the next log record and so on.
     * The user of the cursor can examine the log record using the logLocator and LogRecordHelper class.
     * If next() reaches the end of log file, it returns false as return value. Otherwise, it returns true.
     * For example, there are three log records. 
     *      logRec1, logRec2, logRec3
     * To create log cursor, users have to know the LSN of the first log record and provide it with logCursor constructor. 
     * Then, if the first next() is called, the logCursor and the logLocator indicates the LSN of the first log record(logRec1). 
     * When the second next() is called, the logCursor and the logLocator moves to the next log record(logRec2). 
     * When the third next() is called, the logCursor and the logLocator moves to the last log record(logRec3).
     * When the fourth next() is called, it returns false as return value and the logCursor may or may not move, but 
     * the logLocator is not updated.
     * 
     * @param logLocator
     * @return true if the cursor was successfully moved to the next log record
     *         false if there are no more log records.
     */
    @Override
    public boolean next(LogicalLogLocator logLocator) throws IOException, ACIDException {

        int integerRead = -1;
        boolean logRecordBeginPosFound = false;
        long bytesSkipped = 0;
        
        // find the starting position of the next log record by checking the logMagicNumber.
        while (nextLogicalLogLocator.getMemoryOffset() <= readOnlyBuffer.getSize()
                - logManager.getLogManagerProperties().getLogHeaderSize()) {
            integerRead = readOnlyBuffer.readInt(nextLogicalLogLocator.getMemoryOffset());
            if (integerRead == logManager.getLogManagerProperties().logMagicNumber) {
                logRecordBeginPosFound = true;
                break;
            }
            nextLogicalLogLocator.increaseMemoryOffset(1);
            nextLogicalLogLocator.incrementLsn();
            bytesSkipped++;
            if (bytesSkipped > logManager.getLogManagerProperties().getLogPageSize()) {
                return false; // the maximum size of a log record is limited to
                // a log page size. If we have skipped as many
                // bytes without finding a log record, it
                // indicates an absence of logs any further.
            }
        }

        // load the next log page if the logMagicNumber is not found in the current log page.
        if (!logRecordBeginPosFound) {
            // need to reload the buffer
            long lsnpos = (++bufferIndex * logManager.getLogManagerProperties().getLogBufferSize());
            readOnlyBuffer = getReadOnlyBuffer(lsnpos, logManager.getLogManagerProperties().getLogBufferSize());
            if (readOnlyBuffer != null) {
                nextLogicalLogLocator.setBuffer(readOnlyBuffer);
                nextLogicalLogLocator.setLsn(lsnpos);
                nextLogicalLogLocator.setMemoryOffset(0);
                return next(logLocator);
            } else {
                return false;
            }
        }

        // If you reach here, you found the starting position of the next log record.
        int logLength = logManager.getLogRecordHelper().getLogLength(nextLogicalLogLocator);
        if (logManager.getLogRecordHelper().validateLogRecord(logManager.getLogManagerProperties(), nextLogicalLogLocator)) {
            if (logLocator == null) {
                logLocator = new LogicalLogLocator(0, readOnlyBuffer, -1, logManager);
            }
            logLocator.setLsn(nextLogicalLogLocator.getLsn());
            logLocator.setMemoryOffset(nextLogicalLogLocator.getMemoryOffset());
            logLocator.setBuffer(readOnlyBuffer);
            nextLogicalLogLocator.incrementLsn(logLength);
            nextLogicalLogLocator.setMemoryOffset(nextLogicalLogLocator.getMemoryOffset() + logLength);
        } else {
            throw new ACIDException("Invalid Log Record found ! checksums do not match :( ");
        }
        return logFilter.accept(readOnlyBuffer, logLocator.getMemoryOffset(), logLength);
    }

    /**
     * Returns the filter associated with the cursor.
     * 
     * @return ILogFilter
     */
    @Override
    public ILogFilter getLogFilter() {
        return logFilter;
    }

}

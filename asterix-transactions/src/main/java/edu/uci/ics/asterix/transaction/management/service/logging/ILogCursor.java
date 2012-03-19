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

import java.io.IOException;

import edu.uci.ics.asterix.transaction.management.exception.ACIDException;

/**
 * Provides a cursor over the logs created to date.
 */
public interface ILogCursor {

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
     * @return
     * @throws IOException
     * @throws ACIDException
     */
    public boolean next(LogicalLogLocator logLocator) throws IOException, ACIDException;

    public ILogFilter getLogFilter();

}

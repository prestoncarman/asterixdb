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
package edu.uci.ics.asterix.transaction.management.service.recovery;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.transaction.management.exception.ACIDException;
import edu.uci.ics.asterix.transaction.management.service.logging.FileUtil;
import edu.uci.ics.asterix.transaction.management.service.logging.IBuffer;
import edu.uci.ics.asterix.transaction.management.service.logging.ILogCursor;
import edu.uci.ics.asterix.transaction.management.service.logging.ILogFilter;
import edu.uci.ics.asterix.transaction.management.service.logging.ILogManager;
import edu.uci.ics.asterix.transaction.management.service.logging.ILogRecordHelper;
import edu.uci.ics.asterix.transaction.management.service.logging.LogActionType;
import edu.uci.ics.asterix.transaction.management.service.logging.LogRecordHelper;
import edu.uci.ics.asterix.transaction.management.service.logging.LogType;
import edu.uci.ics.asterix.transaction.management.service.logging.LogUtil;
import edu.uci.ics.asterix.transaction.management.service.logging.LogicalLogLocator;
import edu.uci.ics.asterix.transaction.management.service.logging.PhysicalLogLocator;
import edu.uci.ics.asterix.transaction.management.service.transaction.IResourceManager;
import edu.uci.ics.asterix.transaction.management.service.transaction.MemoryComponentTable;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionContext;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionIDFactory;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionManagementConstants;
import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionProvider;

/**
 * This is the Recovery Manager and is responsible for rolling back a
 * transaction as well as doing a system recovery.
 */
public class RecoveryManager implements IRecoveryManager {

    private static final Logger LOGGER = Logger.getLogger(RecoveryManager.class.getName());
    private final TransactionProvider transactionProvider;
    private final MemoryComponentTable memoryComponentTable;

    /**
     * A file at a known location that contains the LSN of the last log record
     * traversed doing a successful checkpoint.
     * The system has two checkpoint file, left and right.
     * Whenever system starts, first it reads both files and consider the file which contains
     * bigger LSN valid. Then the next checkpoint writes checkpoint information into the other
     * checkpoint file.
     */
    private static final String[] checkpoint_record_file = { "left_last_checkpoint_lsn", "right_last_checkpoint_lsn" };

    private int checkpointFileIndicator;

    private SystemState state;

    public RecoveryManager(TransactionProvider transactionProvider) throws ACIDException {
        this.transactionProvider = transactionProvider;
        this.memoryComponentTable = transactionProvider.getMemoryComponentTable();
    }

    /**
     * returns system state which could be one of the three states: HEALTHY, RECOVERING, CORRUPTED.
     * This state information could be used in a case where more than one thread is running
     * in the bootstrap process to provide higher availability. In other words, while the system
     * is recovered, another thread may start a new transaction with understanding the side effect
     * of the operation, or the system can be recovered concurrently. This kind of concurrency is
     * not supported, yet.
     */
    public SystemState getSystemState() throws ACIDException {
        try {
            if (FileUtil.createFileIfNotExists(checkpoint_record_file[0])) {
                FileUtil.createFileIfNotExists(checkpoint_record_file[1]);
                //If checkpoint_record_file doesn't exist, this is the initial system bootstrap. 
                //The system state is healthy.
                //Do the first checkpoint. It shouldn't be done in efficiency perspective, 
                //but it is done to make checking the healthy state simple and this is only one time overhead.
                state = SystemState.HEALTHY;

                //set the valid checkpoint file indicator to leftCheckpointFile
                checkpointFileIndicator = 0;
                checkpoint(true);
            } else {
                //check if checkponitLogRecord.LSN == lastCheckpointLSN from checkpoint_record_file.
                //If they are same, state is HEALTHY. Otherwise, it's not healthy.
                try {
                    if (LogUtil.initializeLogAnchor(transactionProvider.getLogManager()).getLsn() == this
                            .getLastCheckpointRecordLSN()) {
                        state = SystemState.HEALTHY;
                        //read maxTranasactionId from checkpointFile and 
                        //set the transactionIdSeed to the maxTransactionId.
                        try {
                            TransactionIDFactory.resetTransactionIdSeed(getMaxTransactionIdFromCheckpointRecordFile());
                        } catch (Exception e) {
                            throw new ACIDException("failed to read maxTransactionId from"
                                    + checkpoint_record_file[this.checkpointFileIndicator], e);
                        }
                        //now move the checkpointFileIndicator to the other checkpointFile.
                        checkpointFileIndicator = (checkpointFileIndicator + 1) % 2;
                    } else {
                        state = SystemState.CORRUPTED;
                    }
                } catch (ACIDException ae) {
                    state = SystemState.CORRUPTED;
                    throw new ACIDException("checkpoint_record_file is corrupted" + checkpoint_record_file, ae);
                }
            }
        } catch (IOException ioe) {
            throw new ACIDException(" unable to create checkpoint record file " + checkpoint_record_file, ioe);
        }
        return state;
    }

    /**
     * get low water mark : low water mark is max(minMCTFirstLSN, minDiskLastLSN).
     * The low water mark is used as LSN from where both analysis and redo begins.
     * 
     * @return
     * @throws ACIDException
     */
    private PhysicalLogLocator getLowWaterMark() throws ACIDException {
        long minMCTFirstLSN;
        try {
            minMCTFirstLSN = this.getMinMCTFirstLSNFromCheckpointRecordFile();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            throw new ACIDException("failed to read minMCTFristLSN from"
                    + checkpoint_record_file[this.checkpointFileIndicator], e);
        }
        long minDiskLastLSN = -1;
        long lowWaterMarkLSN = -1;

        //TODO get minDiskLastLSN from all resources.
        if (minMCTFirstLSN > minDiskLastLSN) {
            lowWaterMarkLSN = minMCTFirstLSN;
        } else {
            lowWaterMarkLSN = minDiskLastLSN;
        }

        return new PhysicalLogLocator(lowWaterMarkLSN, this.transactionProvider.getLogManager());
    }

    /**
     * Recovers the crashed system by doing following two phases.
     * First, analysis phase starts reading the log file from the low water mark and
     * construct CTL(Committed Transactions's List) while all log records (from the log water mark)
     * are read up to the end of log file.
     * Second, redo phase starts reading the log file from the low water mark, too and
     * replay committed txns's log records while updating MCT(Memory Component Table) accordingly.
     * Notice! Checkpoint is not done at the end of recovery since the checkpoint is done periodically,
     * we avoid additional IO overhead caused by the checkpoint.
     */
    public SystemState startRecovery(boolean synchronous) throws IOException, ACIDException {

        state = SystemState.RECOVERING;
        HashMap committedTransactions = new HashMap();

        //get the Low Water Mark from which analysis starts reading log records.
        PhysicalLogLocator lowWaterMarkLSN = getLowWaterMark();

        //start analysis
        startAnalysis(lowWaterMarkLSN, committedTransactions);

        //set checkpointFileIndicator to the other file before redo starts
        //since redo may cause checkpoint.
        this.checkpointFileIndicator = (this.checkpointFileIndicator + 1) % 2;

        //start redo
        startRedo(lowWaterMarkLSN, committedTransactions);

        //clean up the recovery 
        committedTransactions.clear();
        state = SystemState.HEALTHY;

        return state;
    }

    private void startAnalysis(PhysicalLogLocator beginLSN, HashMap committedTransactions) throws IOException,
            ACIDException {
        ILogManager logManager = transactionProvider.getLogManager();

        // #. startAnalysis() figure out the max transactionId which has been generated so far.
        //    To do this, it first read the maxTransactionId from checkpointFile, then
        //    while it is reading the log file, the maxTransactionId is updated accordingly.
        long maxTransactionId = 0;
        long currentTransactionId = 0;

        try {
            maxTransactionId = this.getMaxTransactionIdFromCheckpointRecordFile();
        } catch (Exception e) {
            throw new ACIDException("failed to read maxTransactionId from"
                    + checkpoint_record_file[this.checkpointFileIndicator], e);
        }

        // #. set the cursor to the beginLSN
        // LogFilter is not used currently and the accept function always returns true.
        // There is no scenario which requires LogFilter so far. 
        ILogCursor logCursor = logManager.readLog(beginLSN, new ILogFilter() {
            public boolean accept(IBuffer logs, long startOffset, int endOffset) {
                return true;
            }
        });

        // Notice. 
        // The buffer of the LogicalLogLocator is set to the buffer of the LogCursor when LogCursor.next() is called. 
        LogicalLogLocator currentLogicalLogLocator = new LogicalLogLocator(beginLSN.getLsn(), null, -1, logManager);

        //#. read log records up to the end of log file while collecting committed transaction Id.
        boolean logValidity = true;
        byte logType;
        LogRecordHelper logParser = new LogRecordHelper(logManager);
        try {
            while (logValidity) {
                //read the next log record
                logValidity = logCursor.next(currentLogicalLogLocator);
                if (!logValidity) {
                    if (LOGGER.isLoggable(Level.INFO)) {
                        LOGGER.info("[Analysis Phase] reached end of log !");
                    }
                    break;
                }

                logType = logParser.getLogType(currentLogicalLogLocator);
                switch (logType) {
                    case LogType.COMMIT: {
                        currentTransactionId = logParser.getLogTransactionId(currentLogicalLogLocator);
                        //insert a committed transaction id into HashMap, committedTransactions.
                        committedTransactions.put(currentTransactionId, null);
                        //update maxTransactionId if the bigger transactionId is found.
                        if (currentTransactionId > maxTransactionId) {
                            maxTransactionId = currentTransactionId;
                        }
                        break;
                    }
                    case LogType.UPDATE: {
                        //update maxTransactionId if the bigger transactionId is found.
                        currentTransactionId = logParser.getLogTransactionId(currentLogicalLogLocator);
                        if (currentTransactionId > maxTransactionId) {
                            maxTransactionId = currentTransactionId;
                        }
                        break;
                    }
                }
            }
        } catch (Exception e) {
            state = SystemState.CORRUPTED;
            throw new ACIDException("[Analysis Phase] could not recover, corrputed log !", e);
        }

        //reset the transactionIdSeed with maxTransactionId.
        TransactionIDFactory.resetTransactionIdSeed(maxTransactionId);
    }

    private void startRedo(PhysicalLogLocator beginLSN, HashMap committedTransactions) throws IOException,
            ACIDException {
        ILogManager logManager = transactionProvider.getLogManager();

        // #. set the cursor to the beginLSN
        // LogFilter is not used currently and the accept function always returns true.
        // There is no scenario which requires LogFilter so far. 
        ILogCursor logCursor = logManager.readLog(beginLSN, new ILogFilter() {
            public boolean accept(IBuffer logs, long startOffset, int endOffset) {
                return true;
            }
        });

        // Notice. 
        // The buffer of the LogicalLogLocator is set to the buffer of the LogCursor when LogCursor.next() is called. 
        LogicalLogLocator currentLogicalLogLocator = new LogicalLogLocator(beginLSN.getLsn(), null, -1, logManager);

        boolean logValidity = true;
        byte resourceMgrId;
        IResourceManager resourceMgr;
        LogRecordHelper logParser = new LogRecordHelper(logManager);
        try {
            while (logValidity) {

                //read the next log record               
                logValidity = logCursor.next(currentLogicalLogLocator);
                if (!logValidity) {
                    if (LOGGER.isLoggable(Level.INFO)) {
                        LOGGER.info("[Redo Phase] reached end of log !");
                    }
                    break;
                }

                //If the log type is UPDATE and the txnId of the log record exists in the HashMap, committedTransactions, 
                //then make resourceMgr replay the log record.
                //ResourceMgr is in charge of guaranteeing the idempotent property by checking the diskLSN of the resource. 
                if (logParser.getLogType(currentLogicalLogLocator) == LogType.UPDATE
                        && committedTransactions.containsKey(logParser.getLogTransactionId(currentLogicalLogLocator))) {
                    resourceMgrId = logParser.getResourceMgrId(currentLogicalLogLocator);
                    resourceMgr = transactionProvider.getResourceRepository()
                            .getTransactionalResourceMgr(resourceMgrId);
                    if (resourceMgr == null) {
                        throw new ACIDException("[Redo Phase] unknown resource mgr with id " + resourceMgrId);
                    } else {
                        resourceMgr.redo(logParser, currentLogicalLogLocator);
                    }
                }
            }
        } catch (Exception e) {
            state = SystemState.CORRUPTED;
            throw new ACIDException("[Redo Phase] could not recover , corrputed log !", e);
        }
    }

    @Override
    /*
     * Checkpoint executes the following tasks: 
     * write checkpoint log record, 
     * (flush logs up to the checkpoint log record. This is done by LogManager) 
     * save the minMCTFirstLSN, maxTransactionId, and checkpointLSN into checkpoint_log_record file.
     * However, dirty data in the memory is not flushed.
     */
    public void checkpoint(boolean writeToBothFile) throws ACIDException {
        ILogManager logMgr = transactionProvider.getLogManager();
        LogicalLogLocator logLocator = LogUtil.getDummyLogicalLogLocator(logMgr);

        int numberOfWrite;
        if (writeToBothFile) {
            numberOfWrite = 2;
        } else {
            numberOfWrite = 1;
        }

        //write checkpoint log record
        try {
            logMgr.log(logLocator, null, (byte) (-1), 0, LogType.END_CHKPT, LogActionType.NO_OP, 0, null, null);
        } catch (ACIDException ae) {
            if (LOGGER.isLoggable(Level.SEVERE)) {
                LOGGER.severe(" caused exception in checkpoint !");
            }
            throw ae;
        }

        //get minMCTFirstLSN
        long minMCTFirstLSN = this.memoryComponentTable.getMinFirstLSN();
        long maxTransactionId = TransactionIDFactory.getGeneratedMaxTransactionId();

        //write checkpoint log record LSN as well as minMCTFirstLSN twice
        //in order to check whether they are corrupted or not when the recovery starts.
        BufferedWriter bufferedWriter;

        for (int i = 0; i < numberOfWrite; i++) {
            try {

                if (writeToBothFile) {
                    bufferedWriter = new BufferedWriter(new FileWriter(checkpoint_record_file[i]));
                } else {
                    bufferedWriter = new BufferedWriter(new FileWriter(
                            checkpoint_record_file[this.checkpointFileIndicator]));
                }

                bufferedWriter.write("" + minMCTFirstLSN);
                bufferedWriter.newLine();
                bufferedWriter.write("" + minMCTFirstLSN);
                bufferedWriter.newLine();
                bufferedWriter.write("" + maxTransactionId);
                bufferedWriter.newLine();
                bufferedWriter.write("" + maxTransactionId);
                bufferedWriter.newLine();
                bufferedWriter.write("" + logLocator.getLsn());
                bufferedWriter.newLine();
                bufferedWriter.write("" + logLocator.getLsn());
                bufferedWriter.newLine();
                bufferedWriter.flush();
            } catch (IOException ioe) {
                throw new ACIDException(" unable to create check point record", ioe);
            }
        }

        //move the checkpoint file indicator to the other file
        this.checkpointFileIndicator = (this.checkpointFileIndicator + 1) % 2;
    }

    /**
     * Read the checkpoint LSN from both of the checkpoint_record_files.
     * Consider the file which stores bigger LSN a valid checkpoint LSN.
     * 
     * @throws
     * @throws Exception
     */
    private long getLastCheckpointRecordLSN() throws ACIDException {
        String content = null;
        long[] lastCheckpointLSN = { 0, 0 };
        BufferedReader bufferedReader;
        boolean existValidCheckpointLSN = false;
        boolean isCheckpointFileCorrupted = false;

        for (int i = 0; i < 2; i++) {

            try {
                bufferedReader = new BufferedReader(new FileReader(checkpoint_record_file[i]));
            } catch (FileNotFoundException e) {
                if (i == 1 && !existValidCheckpointLSN) {
                    throw new ACIDException("Both checkpoint files are corrupted", e);
                } else {
                    continue;
                }
            }

            //read the minMCTFirstLSN twice and the maxTransactionId twice.
            for (int j = 0; j < 4; j++) {
                try {
                    content = bufferedReader.readLine();
                } catch (IOException e) {
                    if (i == 1 && !existValidCheckpointLSN) {
                        throw new ACIDException("Both checkpoint files are corrupted", e);
                    } else {
                        isCheckpointFileCorrupted = true;
                        break;
                    }
                }
                if (content == null) {
                    if (i == 1 && !existValidCheckpointLSN) {
                        throw new ACIDException("Both checkpoint files are corrupted");
                    } else {
                        isCheckpointFileCorrupted = true;
                        break;
                    }
                }
            }
            if (isCheckpointFileCorrupted) {
                isCheckpointFileCorrupted = false;
                continue;
            }

            //read the first checkpointLSN
            try {
                content = bufferedReader.readLine();
            } catch (IOException e) {
                if (i == 1 && !existValidCheckpointLSN) {
                    throw new ACIDException("Both checkpoint files are corrupted", e);
                } else {
                    continue;
                }
            }
            if (content == null) {
                if (i == 1 && !existValidCheckpointLSN) {
                    throw new ACIDException("Both checkpoint files are corrupted");
                } else {
                    continue;
                }
            } else {
                lastCheckpointLSN[i] = Long.parseLong(content);
            }

            //read the second checkpointLSN
            try {
                content = bufferedReader.readLine();
            } catch (IOException e) {
                if (i == 1 && !existValidCheckpointLSN) {
                    throw new ACIDException("Both checkpoint files are corrupted", e);
                } else {
                    continue;
                }
            }
            if (content == null) {
                if (i == 1 && !existValidCheckpointLSN) {
                    throw new ACIDException("Both checkpoint files are corrupted");
                } else {
                    continue;
                }
            } else {
                if (lastCheckpointLSN[i] == Long.parseLong(content)) {
                    //do nothing!!
                } else {
                    if (i == 1 && !existValidCheckpointLSN) {
                        throw new ACIDException("Both checkpoint files are corrupted");
                    } else {
                        continue;
                    }
                }
            }

            existValidCheckpointLSN = true;
        }

        if (lastCheckpointLSN[0] > lastCheckpointLSN[1]) {
            //set checkpointFileIndicator to left
            this.checkpointFileIndicator = 0;
        } else {
            this.checkpointFileIndicator = 1;
        }

        return lastCheckpointLSN[this.checkpointFileIndicator];
    }

    /**
     * read the minMCTFirstLSN from checkpoint_record_file.
     * 
     * @throws Exception
     */
    private long getMinMCTFirstLSNFromCheckpointRecordFile() throws Exception {
        String content = null;
        long minMCTFirstLSN;
        BufferedReader bufferedReader = new BufferedReader(new FileReader(
                checkpoint_record_file[this.checkpointFileIndicator]));

        // [Flow]
        // #. read the minMCTFirstLSN twice.
        // #. compare the two minMCTFirstLSN and return the value if they are equal. Otherwise, throw exception

        //read the minMCTFirstLSN
        content = bufferedReader.readLine();
        if (content == null) {
            throw new ACIDException(checkpoint_record_file[this.checkpointFileIndicator] + " is corrupted");
        } else {
            minMCTFirstLSN = Long.parseLong(content);
        }

        content = bufferedReader.readLine();
        if (content == null) {
            throw new ACIDException(checkpoint_record_file[this.checkpointFileIndicator] + " is corrupted");
        } else {
            if (minMCTFirstLSN == Long.parseLong(content)) {
                return minMCTFirstLSN;
            } else {
                throw new ACIDException(checkpoint_record_file[this.checkpointFileIndicator] + " is corrupted");
            }
        }
    }

    /**
     * read the maxTransactionId from checkpoint_record_file.
     * 
     * @throws Exception
     */
    private long getMaxTransactionIdFromCheckpointRecordFile() throws Exception {
        String content = null;
        long minMCTFirstLSN;
        BufferedReader bufferedReader = new BufferedReader(new FileReader(
                checkpoint_record_file[this.checkpointFileIndicator]));

        // [Flow]
        // #. read the minMCTFirstLSN twice.
        // #. read the maxTransactionId twice.
        // #. compare the two maxTransactionId and return the value if they are equal. Otherwise, throw exception.

        //read the minMCTFirstLSN
        for (int i = 0; i < 2; i++) {
            content = bufferedReader.readLine();
            if (content == null) {
                throw new ACIDException(checkpoint_record_file[this.checkpointFileIndicator] + " is corrupted");
            }
        }

        //read the first maxTransactionId
        content = bufferedReader.readLine();
        if (content == null) {
            throw new ACIDException(checkpoint_record_file[this.checkpointFileIndicator] + " is corrupted");
        } else {
            minMCTFirstLSN = Long.parseLong(content);
        }

        //read the second maxTransactionId and compare the values
        content = bufferedReader.readLine();
        if (content == null) {
            throw new ACIDException(checkpoint_record_file[this.checkpointFileIndicator] + " is corrupted");
        } else {
            if (minMCTFirstLSN == Long.parseLong(content)) {
                return minMCTFirstLSN;
            } else {
                throw new ACIDException(checkpoint_record_file[this.checkpointFileIndicator] + " is corrupted");
            }
        }
    }

    /**
     * Rollback a transaction
     * 
     * @see edu.uci.ics.transaction.management.service.recovery.IRecoveryManager# rollbackTransaction (edu.uci.ics.transaction.management.service.transaction .TransactionContext)
     */
    @Override
    public void rollbackTransaction(TransactionContext txnContext) throws ACIDException {
        ILogManager logManager = transactionProvider.getLogManager();
        ILogRecordHelper logParser = logManager.getLogRecordHelper();

        // Obtain the last log record written by the transaction
        PhysicalLogLocator lsn = txnContext.getLastLogLocator();
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info(" rollbacking transaction log records at lsn " + lsn.getLsn());
        }

        // check if the transaction actually wrote some logs.
        if (lsn.getLsn() == TransactionManagementConstants.LogManagerConstants.TERMINAL_LSN) {
            if (LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info(" no need to roll back as there were no operations by the transaction "
                        + txnContext.getTransactionID());
            }
            return;
        }

        // a dummy logLocator instance that is re-used during rollback
        LogicalLogLocator logLocator = LogUtil.getDummyLogicalLogLocator(logManager);
        byte logType;
        IResourceManager resourceMgr;
        boolean moreLogs;
        while (true) {
            try {
                // read the log record at the given position
                logLocator = logManager.readLog(lsn);
            } catch (Exception e) {
                e.printStackTrace();
                state = SystemState.CORRUPTED;
                throw new ACIDException(" could not read log at lsn :" + lsn, e);
            }

            logType = logParser.getLogType(logLocator);
            if (LOGGER.isLoggable(Level.FINE)) {
                LOGGER.fine(" reading LSN value inside rollback transaction method " + txnContext.getLastLogLocator()
                        + " txn id " + logParser.getLogTransactionId(logLocator) + " log type  " + logType);
            }

            switch (logType) {
                case LogType.UPDATE:

                    // extract the resource manager id from the log record.
                    byte resourceMgrId = logParser.getResourceMgrId(logLocator);
                    if (LOGGER.isLoggable(Level.FINE)) {
                        LOGGER.fine(logParser.getLogRecordForDisplay(logLocator));
                    }

                    // look up the repository to get the resource manager
                    resourceMgr = transactionProvider.getResourceRepository()
                            .getTransactionalResourceMgr(resourceMgrId);
                    if (resourceMgr == null) {
                        throw new ACIDException(txnContext, " unknown resource manager " + resourceMgrId);
                    } else {
                        resourceMgr.undo(logParser, logLocator);
                    }
                    break;

                case LogType.COMMIT:
                    throw new ACIDException(txnContext, " cannot rollback commmitted transaction");

                default:
                    throw new ACIDException(txnContext, " unexpected log type: " + logType);
            }

            // follow the previous LSN pointer to get the previous log record
            // written by the transaction
            // If the return value is true, the logLocator, it indicates that
            // the logLocator object has been
            // appropriately set to the location of the next log record to be
            // processed as part of the roll back
            moreLogs = logParser.getPreviousLsnByTransaction(lsn, logLocator);
            if (!moreLogs) {
                // no more logs to process
                break;
            }
        }
    }

}

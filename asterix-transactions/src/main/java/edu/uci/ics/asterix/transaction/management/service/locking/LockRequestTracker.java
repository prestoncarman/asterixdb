package edu.uci.ics.asterix.transaction.management.service.locking;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class LockRequestTracker {
    HashMap<Integer, StringBuilder> jobRequestHistory; //per job
    StringBuilder globalRequestHistory;

    public LockRequestTracker() {
        globalRequestHistory = new StringBuilder();
        jobRequestHistory = new HashMap<Integer, StringBuilder>();
    }

    public void addEvent(String msg, LockRequest request) {
        int jobId = request.txnContext.getJobId().getId();
        StringBuilder jobHistory = jobRequestHistory.get(jobId);

        //update jobHistory
        if (jobHistory == null) {
            jobHistory = new StringBuilder();
        }
        jobHistory.append(request.prettyPrint()).append("--> ").append(msg).append("\n");
        jobRequestHistory.put(jobId, jobHistory);

        //handle global request queue
        globalRequestHistory.append(request.prettyPrint()).append("--> ").append(msg).append("\n");
    }

    public String getGlobalRequestHistory() {
        return globalRequestHistory.toString();
    }

    public String getLocalRequestHistory() {
        StringBuilder history = new StringBuilder();
        Set<Entry<Integer, StringBuilder>> s = jobRequestHistory.entrySet();
        Iterator<Entry<Integer, StringBuilder>> iter = s.iterator();
        while (iter.hasNext()) {
            Map.Entry<Integer, StringBuilder> entry = (Map.Entry<Integer, StringBuilder>) iter.next();
            history.append(entry.getValue().toString());
        }
        return history.toString();
    }
}
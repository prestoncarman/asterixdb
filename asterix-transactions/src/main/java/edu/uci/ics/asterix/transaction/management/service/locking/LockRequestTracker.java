package edu.uci.ics.asterix.transaction.management.service.locking;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class LockRequestTracker {
    ArrayList<LockRequest> globalRequestQueue;
    HashMap<Integer, Pair<ArrayList<LockRequest>, StringBuilder>> localRequestQueue; //per job
    StringBuilder globalRequestHistory;

    public LockRequestTracker() {
        globalRequestQueue = new ArrayList<LockRequest>();
        globalRequestHistory = new StringBuilder();
        localRequestQueue = new HashMap<Integer, Pair<ArrayList<LockRequest>, StringBuilder>>();
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void addRequest(LockRequest request) {
        int jobId = request.txnContext.getJobId().getId();
        globalRequestQueue.add(request);
        Pair pair = localRequestQueue.get(jobId);

        //handle per-job request queue
        if (pair == null) {
            ArrayList<LockRequest> jobRequestList = new ArrayList<LockRequest>();
            StringBuilder jobRequestHistory = new StringBuilder();
            pair = new Pair(jobRequestList, jobRequestHistory);
        }
        ((ArrayList<LockRequest>) (pair.requestList)).add(request);
        ((StringBuilder) (pair.requestHistory)).append(request.prettyPrint());
        localRequestQueue.put(jobId, pair);

        //handle global request queue
        globalRequestQueue.add(request);
        globalRequestHistory.append(request.prettyPrint());
    }

    public String getGlobalRequestHistory() {
        return globalRequestHistory.toString();
    }

    public String getLocalRequestHistory() {
        StringBuilder history = new StringBuilder();
        Set<Entry<Integer, Pair<ArrayList<LockRequest>, StringBuilder>>> s = localRequestQueue.entrySet();
        Iterator<Entry<Integer, Pair<ArrayList<LockRequest>, StringBuilder>>> iter = s.iterator();
        while (iter.hasNext()) {
            Map.Entry<Integer, Pair<ArrayList<LockRequest>, StringBuilder>> entry = (Map.Entry<Integer, Pair<ArrayList<LockRequest>, StringBuilder>>) iter
                    .next();
            history.append(entry.getValue().requestHistory.toString());
        }
        return history.toString();
    }
}

class Pair<T1, T2> {

    public T1 requestList;
    public T2 requestHistory;

    public Pair(T1 list, T2 history) {
        this.requestList = list;
        this.requestHistory = history;
    }
}

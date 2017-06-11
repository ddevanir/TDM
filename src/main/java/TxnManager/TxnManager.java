package TxnManager;

import DataManager.Data;
import DataManager.DataManager;
import LoggingManager.Log;
import LoggingManager.LoggingManager;
import org.json.JSONObject;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by beep on 5/26/17.
 */
 public class TxnManager implements ITxnManager{
    public static ArrayList<Log> bufferLog = new ArrayList<Log>(); //infinite Log Buffer
    public static ReentrantLock logBufferLock = new ReentrantLock();
    public static ArrayList<Data> bufferData = new ArrayList<Data>(); //infinite Data Buffer
    public static ReentrantLock dataBufferLock = new ReentrantLock();

    private LoggingManager loggingManager ;
    private DataManager dataManager;
    private FlushMonitor flushMonitor;
    private Thread flushMonitorThread;
    public static int globalLSNCounter = 0;
    private Boolean bFlushOnCommit = false;
    public static Map<String, ArrayList<Integer>> policyLsnMap = new HashMap<String, ArrayList<Integer>>();
    //private final int BUFFER_LIMIT = 5;

    /*Returns Transaction ID
    * UNDO not required
    * REDO is only required
    * */
    // flustime in secs
    public TxnManager(int flushtime) {
        loggingManager = new LoggingManager();
        dataManager = new DataManager();
        recovery();
        if (flushtime > 0){
            bFlushOnCommit = false;
            flushMonitor = new FlushMonitor(flushtime, loggingManager, dataManager);
            flushMonitorThread = new Thread(flushMonitor);
            flushMonitorThread.start();
        }
        else if ( flushtime == 0) {
            bFlushOnCommit = true;
        }
    }

    public void recovery() {
        ArrayList<String> txnID = getPendingTID();
        ArrayList<Integer> lsnList = loggingManager.queryFirstLsn();

        addRecoveryData(lsnList);
    }



    private void addRecoveryData(ArrayList<Integer> lsnList) {
        for(Integer lsn : lsnList) {
            Integer nextLsn = lsn;
            while (nextLsn != -1) {
                JSONObject record = loggingManager.searchPayload(nextLsn);
                String payload = null;
                if(record.has("payload") )
                    payload = record.getString("payload");
                if(record.has("nextLsn"))
                    nextLsn = record.getInt("nextLsn");
                if (payload != null) {
                    dataManager.updateData(payload);
                }

                System.out.println(nextLsn);
            }
            loggingManager.removeFirstLsn(lsn);
        }
    }

    private ArrayList<String> getPendingTID() {
        ArrayList<String> pendingTIDs = loggingManager.getPendingTransaction();

        for(String pending : pendingTIDs) {
            System.out.println("Pending Transaction :" + pending);
        }

        return pendingTIDs;
    }

    public String begin() {
        TxnManager.AcquireLocks();
        String txnID = UUID.randomUUID().toString();
        globalLSNCounter++;
        Log log = loggingManager.createLogRecord(txnID, Log.logType.BEGIN, null);
        bufferLog.add(log);
        loggingManager.insertPendingTransaction(txnID);
        TxnManager.ReleaseLocks();
        return txnID;
    }

    public void writePolicy(String txnID, JSONObject payload) {
        TxnManager.AcquireLocks();
        globalLSNCounter++;
        Log log = loggingManager.createLogRecord(txnID, Log.logType.RECORD, payload);
        bufferLog.add(log);

        Data data = dataManager.createDataRecord(payload, Data.dataType.PENDING);
        bufferData.add(data);

        InsertpolicyLsnMap(DataManager.getPolicyID(payload),log.getLSN());
        TxnManager.ReleaseLocks();
    }


    private static void InsertpolicyLsnMap(String policyID, Integer lsn) {
        if(policyLsnMap.containsKey(policyID)) {
            ArrayList<Integer> value = policyLsnMap.get(policyID);
            value.add(lsn);
            policyLsnMap.put(policyID, value);
        } else {
            ArrayList<Integer> lsnList = new ArrayList<Integer>();
            lsnList.add(lsn);
            policyLsnMap.put(policyID, lsnList);
        }
    }

//    private void flushData() {
//        for(Log buffer : bufferLog) {
//            loggingManager.writeLog(buffer);
//            if(buffer.getType() == Log.logType.RECORD) {
//                dataManager.writeData(buffer.getPayLoad());
//            }
//        }
//        bufferLog.clear();
//    }

    private void flushLogBuffer() {
        for(Log buffer : bufferLog) {
            loggingManager.writeLog(buffer);
        }
        bufferLog.clear();
    }

    public void commit(String txnID) {
        TxnManager.AcquireLocks();
        globalLSNCounter++;
        Log log = loggingManager.createLogRecord(txnID, Log.logType.COMMIT, null);
        bufferLog.add(log);

        Log firstLogObject = bufferLog.get(0);
        loggingManager.insertFirstLsn(firstLogObject);

        flushLogBuffer();

        changeDataType(Data.dataType.COMMITTED);
        loggingManager.removePendingTid(txnID);
        TxnManager.ReleaseLocks();
        if(bFlushOnCommit) {
            flush();
        }
    }

    private void changeDataType(Data.dataType type) {
        int end = bufferData.size() - 1;

        for(int i = end; i >= 0; i--) {
            Data data = bufferData.get(i);

            if(data.getType() == Data.dataType.PENDING) {
                data.setType(type);
            } else if(data.getType() == Data.dataType.COMMITTED) {
                break;
            }
        }
    }

    private void deleteAbortedDataType(Data.dataType type) {
        int end = bufferData.size() - 1;

        for(int i = end; i >= 0; i--) {
            Data data = bufferData.get(i);

            if(data.getType() == Data.dataType.ABORTED) {
                bufferData.remove(data);
            } else if(data.getType() == Data.dataType.COMMITTED) {
                break;
            }
        }
    }

    private long getEpoch(String date) throws ParseException {
        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date dateEpoch = dateFormat.parse(date);
        long epoch = dateEpoch.getTime();
        return epoch;
    }



    public void abort(String txnID) {
        TxnManager.AcquireLocks();
        globalLSNCounter++;
        Log log = loggingManager.createLogRecord(txnID, Log.logType.ABORT, null);
        bufferLog.add(log);
        flushLogBuffer();
        changeDataType(Data.dataType.ABORTED);
        deleteAbortedDataType(Data.dataType.ABORTED);
        loggingManager.removePendingTid(txnID);
        TxnManager.ReleaseLocks();
        if(bFlushOnCommit) {
            flush();
        }
    }

    public void flush() {
        TxnManager.AcquireLocks();
        ArrayList<Data> markDelete = new ArrayList<Data>();
        for(Data data : bufferData) {
            if(data.getType() == Data.dataType.COMMITTED) {
                //writePolicy(data);
                dataManager.updateData(data);
                loggingManager.removeFirstLsn();
                JSONObject payload = data.getPayLoad();
                String policyID = DataManager.getPolicyID(payload);
                TxnManager.policyLsnMap.remove(policyID);
                markDelete.add(data);
            } else {
                continue;
            }
        }
        for (Data data : markDelete ){
            bufferData.remove(data);
        }
        //bufferData.clear();
        TxnManager.ReleaseLocks();
    }

//    private void writePolicy(Data data) {
//
//
//        if(dataManager.PolicyIDPresent(data) == true) {
//            String policyID = DataManager.getPolicyID(data.getPayLoad());
//            dataManager.updateData(data, policyID);
//        } else {
//            dataManager.writeNewData(data);
//        }
//    }

    public void timeTraversal(String ts) throws ParseException {
        long tsEpoch = getEpoch(ts);
        TxnManager.AcquireLocks();
        List<JSONObject> records = loggingManager.getTimeTraversalRecords(tsEpoch);
        if(records.size() != 0) {
            // Going back in time
            ArrayList<Integer> lsnList = new ArrayList<Integer>();
            int i = 0;
            for (JSONObject rec : records) {

                Log.logType type = Log.getTypeFromString((String) rec.get("type"));
                if (type == Log.logType.BEGIN) {
                    break;
                }
                i = i + 1;
            }

            for (int j = records.size() - 1; j >= i; j--) {
                JSONObject rec = records.get(j);
                lsnList.add(rec.getInt("LSN"));
                Log.logType type = Log.getTypeFromString((String) rec.get("type"));
                if (type != Log.logType.RECORD) {
                    continue;
                }
                if (rec.has("prevPayload")) {
                    JSONObject payload = rec.getJSONObject("prevPayload");
                    dataManager.updateData(payload.toString());
                } else {
                    JSONObject payload = rec.getJSONObject("payload");
                    String policyID = DataManager.getPolicyID((payload));
                    dataManager.deletePolicy(policyID);
                }
            }
            loggingManager.markAsTimeTraversed(lsnList);
        }
        else{
            // go forward in time
            List<JSONObject> futureRecords = loggingManager.getFutureTimeTraversalRecords(tsEpoch);

            ArrayList<Integer> lsnList = new ArrayList<Integer>();
            int i = 0;
            //Collections.reverse(futureRecords);
            for (JSONObject rec : futureRecords) {

                Log.logType type = Log.getTypeFromString((String) rec.get("type"));
                if (type == Log.logType.BEGIN) {
                    break;
                }
                i = i + 1;
            }

            int last = futureRecords.size() - 1;
            for (int j = futureRecords.size() - 1; j >= i; j--){
                JSONObject rec = futureRecords.get(j);

                Log.logType type = Log.getTypeFromString((String) rec.get("type"));
                if (type == Log.logType.COMMIT) {
                    break;
                }
                last = j;
            }

            for (int j = i  ; j <= last ; j++) {
                JSONObject rec = futureRecords.get(j);
                lsnList.add(rec.getInt("LSN"));
                Log.logType type = Log.getTypeFromString((String) rec.get("type"));
                if (type != Log.logType.RECORD) {
                    continue;
                }
                if (rec.has("payload")) {
                    JSONObject payload = rec.getJSONObject("payload");
                    dataManager.updateData(payload.toString());
                }
            }
            loggingManager.markAsNotTimeTraversed(lsnList);
        }
        TxnManager.ReleaseLocks();
    }

    public void viewHistory(String timestamp) throws ParseException {
        long epochTs = getEpoch(timestamp);
        TxnManager.AcquireLocks();

        // A part
        List<JSONObject> jsonListA = dataManager.getRecords(epochTs);

        dataManager.addNewData(jsonListA);
        // B part
        List<JSONObject> jsonListB = loggingManager.getTimeTraversalRecords(epochTs);
        ArrayList<Integer> lsnList = new ArrayList<Integer>();
        int i = 0;
        for(JSONObject rec : jsonListB) {
            i = i+1;
            Log.logType type = Log.getTypeFromString((String) rec.get("type"));
            if(type == Log.logType.BEGIN){
                break;
            }
        }

        for(int j = jsonListB.size() - 1 ; j >=i  ; j--){
            JSONObject rec = jsonListB.get(j);
            lsnList.add(rec.getInt("LSN"));
            Log.logType type = Log.getTypeFromString((String) rec.get("type"));
            if(type != Log.logType.RECORD){
                continue;
            }
            if(rec.has("prevPayload")){
                JSONObject payload = rec.getJSONObject("prevPayload");
                dataManager.updateNewData(payload);
            }
            else{
                JSONObject payload = rec.getJSONObject("payload");
                String policyID = DataManager.getPolicyID((payload));
                dataManager.deletePolicyFromOld(policyID);
            }
        }
        TxnManager.ReleaseLocks();
   }

    public static void AcquireLocks(){
        TxnManager.logBufferLock.lock();
        TxnManager.dataBufferLock.lock();
    }

    public static void ReleaseLocks(){
        TxnManager.logBufferLock.unlock();
        TxnManager.dataBufferLock.unlock();
    }
}
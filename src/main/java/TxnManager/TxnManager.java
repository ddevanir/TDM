package TxnManager;

import DataManager.DataManager;
import LoggingManager.Log;
import DataManager.Data;
import LoggingManager.LoggingManager;
import com.mongodb.BasicDBObject;
import org.json.JSONObject;

import java.sql.Timestamp;
import java.util.*;

/**
 * Created by beep on 5/26/17.
 */
public class TxnManager implements ITxnManager{
    public static ArrayList<Log> bufferLog = new ArrayList<Log>(); //infinite Log Buffer
    private static ArrayList<Data> bufferData = new ArrayList<Data>(); //infinite Data Buffer
    private LoggingManager loggingManager ;
    private DataManager dataManager;
    public static int globalLSNCounter = 0;
    public static Map<String, ArrayList<Integer>> policyLsnMap = new HashMap<String, ArrayList<Integer>>();
    //private final int BUFFER_LIMIT = 5;

    /*Returns Transaction ID
    * UNDO not required
    * REDO is only required
    * */

    public TxnManager() {
        loggingManager = new LoggingManager();
        dataManager = new DataManager();
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
        String txnID = UUID.randomUUID().toString();
        globalLSNCounter++;
        Log log = loggingManager.createLogRecord(txnID, Log.logType.BEGIN, null);
        bufferLog.add(log);
        loggingManager.insertPendingTransaction(txnID);
        return txnID;
    }

    public void writePolicy(String txnID, JSONObject payload) {
        globalLSNCounter++;
        Log log = loggingManager.createLogRecord(txnID, Log.logType.RECORD, payload);
        bufferLog.add(log);

        Data data = dataManager.createDataRecord(payload, Data.dataType.PENDING);
        bufferData.add(data);

        InsertpolicyLsnMap(DataManager.getPolicyID(payload),log.getLSN());
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

    private void flushData() {
        for(Log buffer : bufferLog) {
            loggingManager.writeLog(buffer);
            if(buffer.getType() == Log.logType.RECORD) {
                dataManager.writeData(buffer.getPayLoad());
            }
        }
        bufferLog.clear();
    }

    private void flushLogBuffer() {
        for(Log buffer : bufferLog) {
            loggingManager.writeLog(buffer);
        }
        bufferLog.clear();
    }

    public void commit(String txnID) {
        globalLSNCounter++;
        Log log = loggingManager.createLogRecord(txnID, Log.logType.COMMIT, null);
        bufferLog.add(log);

        Log firstLogObject = bufferLog.get(0);
        loggingManager.insertFirstLsn(firstLogObject);

        flushLogBuffer();

        changeDataType(Data.dataType.COMMITTED);
        loggingManager.removePendingTid(txnID);

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

    public void abort(String txnID) {
        globalLSNCounter++;
        Log log = loggingManager.createLogRecord(txnID, Log.logType.ABORT, null);
        bufferLog.add(log);
        flushLogBuffer();
        changeDataType(Data.dataType.ABORTED);
        deleteAbortedDataType(Data.dataType.ABORTED);
    }

    public void flush() {
        for(Data data : bufferData) {
            if(data.getType() == Data.dataType.COMMITTED) {
                //writePolicy(data);
                dataManager.updateData(data);
                loggingManager.removeFirstLsn();
                JSONObject payload = data.getPayLoad();
                String policyID = DataManager.getPolicyID(payload);
                TxnManager.policyLsnMap.remove(policyID);
            } else {
                continue;
            }
        }
        bufferData.clear();
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

    public void timeTraversal(Timestamp ts) {
        List<JSONObject> records = loggingManager.getTimeTraversalRecords(ts);
        Boolean bInclude = true;
        List<JSONObject> validRecords = new ArrayList<JSONObject>();
        List<JSONObject> tempRecords = new ArrayList<JSONObject>();
        int i = 0;
        for(JSONObject rec : records) {
            i = i+1;
            Log.logType type = (Log.logType)rec.get("type");
            if(type == Log.logType.BEGIN){
                break;
            }
        }

        for(int j = i ; j < records.size() ; j++){
            JSONObject rec = records.get(j);
            if(rec.has("prevPayload")){
                String payload = rec.getString("prevPayload");
                dataManager.updateData(payload);
            }
            else{
                String payload = rec.getString("payload");
                String policyID = DataManager.getPolicyID(new JSONObject(payload));
                dataManager.deletePolicy(policyID);
            }
        }
    }
}
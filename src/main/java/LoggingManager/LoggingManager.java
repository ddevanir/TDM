package LoggingManager;

import DataManager.DataManager;
import TxnManager.TxnManager;
import org.json.JSONObject;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by beep on 5/27/17.
 */
public class LoggingManager implements ILoggingManager{
    private MongoDB mongoDB;
    //public Integer LSN;
    //private TxnManager txnManager;

    public LoggingManager() {
        mongoDB = new MongoDB();
    }

    public boolean writeLog(Log log) {
        JSONObject json = log.getJSON();
        if(mongoDB.insertLog(json) == true) {
            System.out.println("Insertion to Log DB is successfull");
            return true;
        }
        return false;
    }

    public boolean flushLog(ArrayList<Log> logs) {
        return false;
    }

    public Log createLogRecord(String txnID, Log.logType type, JSONObject payload) {
        Log log = new Log(txnID, type, payload);
        JSONObject prevPayload = findPrevPayload(log, payload);
        log.addPrevPayload(prevPayload);
        return log;
    }

    public void insertPendingTransaction(String txnID) {
        mongoDB.insertPendingTransaction(txnID);
    }

    public ArrayList<String> getPendingTransaction() {
        ArrayList<String> pendingTID = mongoDB.getPendingTransaction();
        return pendingTID;
    }

    public void insertFirstLsn(Log log) {
        String tid = log.getTID();
        Integer lsn = log.getLSN();
        mongoDB.insertFirstLsn(tid, lsn);
    }

    public void removeFirstLsn(Integer lsn) {
        mongoDB.removeFirstLsn(lsn);
    }
    // Delete All documents from collection Using blank BasicDBObject
    public void removeFirstLsn() {
        mongoDB.removeFirstLsn();
    }

    public ArrayList<Integer> queryFirstLsn() {
        return mongoDB.queryFirstLsn();
    }

    public JSONObject searchPayload(Integer lsn) {
        return mongoDB.searchPayload(lsn);
    }

    private JSONObject findPrevPayload(Log log, JSONObject payload) {
        JSONObject prevPayload = null;

        if (log.getType() == Log.logType.RECORD) {
            String policyID = DataManager.getPolicyID(payload);
            ArrayList<Integer> value = TxnManager.policyLsnMap.get(policyID);
            //found
            if (value != null) {
                Integer lastLsnValue = value.get(value.size() - 1);
                prevPayload = mongoDB.queryPayload(lastLsnValue);

            } else { //not found
                prevPayload = mongoDB.queryPayload(policyID);
            }
        }
        return prevPayload;
    }

    public void removePendingTid(String txnID) {
        mongoDB.removePendingTid(txnID);
    }

    public String getPolicyID(String lsn) {
        return mongoDB.getPolicyID(lsn);
    }

    public void timeTraversal(Timestamp timestamp) {
        mongoDB.sortRecordDescendingOrder();
        mongoDB.updateDocTimeTraversal(timestamp);
    }

    public List<JSONObject> getTimeTraversalRecords(Timestamp ts){
        return mongoDB.getTimeTraversalRecords(ts);
    }
}

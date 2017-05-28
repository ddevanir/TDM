package TxnManager;

import DataManager.DataManager;
import LoggingManager.Log;
import LoggingManager.LoggingManager;
import com.mongodb.DBCursor;

import java.util.ArrayList;
import java.util.UUID;

/**
 * Created by beep on 5/26/17.
 */
public class TxnManager implements ITxnManager{
    private ArrayList<Log> bufferLogData = new ArrayList<Log>(5);
    private LoggingManager loggingManager = new LoggingManager();
    private DataManager dataManager = new DataManager();
    private final int BUFFER_LIMIT = 5;

    public String begin() {
        String txnID = UUID.randomUUID().toString();
        Log log = new Log(txnID, Log.logType.BEGIN);
        bufferLogData.add(log);
        return txnID;
    }

    public void writeLogData(String transID, String payload) {
        Log log = new Log(transID, Log.logType.RECORD, payload);
        bufferLogData.add(log);

        if(bufferLogData.size() == BUFFER_LIMIT) {
            flushData();
        }
    }

    private void flushData() {
        for(Log buffer : bufferLogData) {
            loggingManager.writeLog(buffer);
            if(buffer.getType() == Log.logType.RECORD) {
                dataManager.writeData(buffer.getPayLoad());
            }
        }
        bufferLogData.clear();
    }

    private void flushLog() {
        for(Log buffer : bufferLogData) {
            loggingManager.writeLog(buffer);
        }
        bufferLogData.clear();
    }

    public void commit(String txnID) {
        Log log = new Log(txnID, Log.logType.COMMIT);
        bufferLogData.add(log);
        flushData();
    }

    public void abort(String txnID) {
        Log log = new Log(txnID, Log.logType.ABORT);
        bufferLogData.add(log);
        flushLog();
        dataManager.abort(txnID);
    }
}
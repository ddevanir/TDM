package LoggingManager;

import org.json.JSONObject;

import java.util.ArrayList;

/**
 * Created by beep on 5/21/17.
 */

public interface ILoggingManager {
    /*
    write logs given in the buffer to the database.
    @param: Array list of logs
    @return true:  dumping in the database is successful
            false: unsuccessful
     */
    public boolean writeLog(Log log);

    /*
    Flushes log which is there in the buffer to the database
    @param: Array list of logs
    @return true: success
            false: failure
     */
    public boolean flushLog(ArrayList<Log> logs);

    /* Creates log object with given parameters
    */
    public Log createLogRecord(String txnID, Log.logType type, JSONObject payload);

}
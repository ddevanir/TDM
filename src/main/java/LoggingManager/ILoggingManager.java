package LoggingManager;

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
    public boolean writeLog(ArrayList<Log> logs);

    /*
    Flushes log which is there in the buffer to the database
    @param: Array list of logs
    @return true: success
            false: failure
     */
    public boolean flushLog(ArrayList<Log> logs);
}
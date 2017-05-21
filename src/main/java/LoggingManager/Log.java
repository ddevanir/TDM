package LoggingManager;

import java.sql.Timestamp;

/**
 * Created by beep on 5/21/17.
 */
public class Log {
    private String TID;
    private Integer LSN;
    private Integer prevLSN;
    private Integer nextLSN;
    private String payload;
    private enum logType {
        BEGIN,
        COMMIT,
        ABORT,
        RECORD
    }
    private logType type;
    private Timestamp timestamp;

    public Log(String TID, logType type,String payload) {
        this.TID = TID;
        this.type = type;
        this.payload = payload;
        timestamp = new Timestamp(System.currentTimeMillis());
    }
}

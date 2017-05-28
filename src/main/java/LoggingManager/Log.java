package LoggingManager;

import org.json.JSONObject;

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
    public enum logType {
        BEGIN,
        COMMIT,
        ABORT,
        RECORD
    }

    private logType type;
    private Timestamp timestamp;

    public Log(String TID, logType type, String payload) {
        this.TID = TID;
        this.type = type;
        timestamp = new Timestamp(System.currentTimeMillis());

        JSONObject obj = new JSONObject(payload);
        obj.put("TID",this.TID);
        this.payload = obj.toString();
    }

    public Log(String TID, logType type) {
        this.TID = TID;
        this.type = type;
    }

    public String getPayLoad() {
        return this.payload;
    }

    public Log.logType getType() {
        return type;
    }

    public JSONObject getJSON() {
        JSONObject json = new JSONObject();
        json.put("TID", this.TID);
        json.put("LSN", this.LSN);
        json.put("PrevLSN", this.prevLSN);
        json.put("NextLSN", this.nextLSN);
        json.put("type", this.type);
        json.put("timestamp", this.timestamp);
        if(this.type == logType.RECORD) {
            json.put("payload", new JSONObject(payload));
        }
        return json;
    }
}

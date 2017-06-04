package LoggingManager;

import TxnManager.TxnManager;
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
    private JSONObject payload;
    private JSONObject prevPayload;
    public enum logType {
        BEGIN,
        COMMIT,
        ABORT,
        RECORD
    }

    private logType type;
    private Timestamp timestamp;

    public Log(String TID, logType type, JSONObject payload) {
        this.TID = TID;
        this.type = type;
        timestamp = new Timestamp(System.currentTimeMillis());
        this.LSN = TxnManager.globalLSNCounter;
        this.prevLSN = this.LSN - 1;
        this.nextLSN = this.LSN + 1;

        if(type == logType.BEGIN)
            this.prevLSN = -1;
        if(type == logType.COMMIT || type == logType.ABORT)
            this.nextLSN = -1;


//        JSONObject obj = new JSONObject(payload);
//        obj.put("TID",this.TID);
        this.payload = payload;
    }

    public Log(String TID, logType type) {
        this.TID = TID;
        this.type = type;
    }

    public JSONObject getPayLoad() {
        return this.payload;
    }

    public Integer getLSN() {
        return this.LSN;
    }

    public String getTID() {
        return this.TID;
    }

    public Log.logType getType() {
        return this.type;
    }

    public JSONObject getJSON() {
        JSONObject json = new JSONObject();
        json.put("TID", this.TID);
        json.put("LSN", this.LSN);
        json.put("PrevLSN", this.prevLSN);
        json.put("NextLSN", this.nextLSN);
        json.put("type", this.type);
        json.put("prevPayload", this.prevPayload);
        json.put("timestamp", this.timestamp);
        if(this.type == logType.RECORD) {
            json.put("payload", payload);
        }
        return json;
    }

    public void addPrevPayload(JSONObject prevPayload) {
        this.prevPayload = prevPayload;
    }
}

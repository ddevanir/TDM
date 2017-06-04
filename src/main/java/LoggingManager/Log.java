package LoggingManager;

import TxnManager.TxnManager;
import org.json.JSONObject;
import org.omg.CosNaming.NamingContextExtPackage.StringNameHelper;

import java.sql.Timestamp;
import java.util.Date;

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
    private boolean timeTraversed;
    public enum logType {
        BEGIN,
        COMMIT,
        ABORT,
        RECORD,
        INVALID
    }

    private logType type;
    private long timestamp;

    public Log(String TID, logType type, JSONObject payload) {
        this.TID = TID;
        this.type = type;
        //timestamp = new Timestamp(System.currentTimeMillis());
        timestamp = System.currentTimeMillis();
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
        this.timeTraversed = false;
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
        json.put("timeTraversed", this.timeTraversed);
        return json;
    }

    public void addPrevPayload(JSONObject prevPayload) {
        this.prevPayload = prevPayload;
    }

    public static Log.logType getTypeFromString (String sType){
        Log.logType ret = logType.INVALID;
        if(sType.equals("BEGIN")){
            ret =  logType.BEGIN;
        }
        if(sType.equals("RECORD")){
            ret =  logType.RECORD;
        }
        if(sType.equals("ABORT")){
            ret = logType.ABORT;
        }
        if(sType.equals("COMMIT")){
            ret =  logType.COMMIT;
        }
        return ret;

    }
}

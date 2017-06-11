package DataManager;

import org.json.JSONObject;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

import static TxnManager.TxnManager.policyLsnMap;

/**
 * Created by beep on 6/2/17.
 */
public class Data {
    private JSONObject payload;

    public enum dataType {
        PENDING,
        COMMITTED,
        ABORTED
    }
    private dataType type;

    public Data(JSONObject payload, dataType type) {
        this.type = type;
        payload.put("lastUpdateTs",System.currentTimeMillis());
        this.payload = payload;
    }

    public Data.dataType getType() {
        return type;
    }

    public void setType(dataType type) {
        this.type = type;
    }

    public JSONObject getPayLoad() {
        return this.payload;
    }

    public JSONObject getJSON() {
        JSONObject json = new JSONObject();
        //json.put("type", this.type);
        //json.put("payload", new JSONObject(payload));
        return payload;
    }

//    public void removePolicyID(JSONObject payload) {
//        JSONObject id = payload.getJSONObject("payload");
//        String policyID = id.getString("id");
//
//        Iterator<Map.Entry<String, ArrayList<Integer>>> it = policyLsnMap.entrySet().iterator();
//        for(; it.hasNext(); ) {
//            Map.Entry<String, ArrayList<Integer>> entry = it.next();
//            if(entry.getKey().equals(policyID)) {
//                it.remove();
//            }
//        }
//    }
}

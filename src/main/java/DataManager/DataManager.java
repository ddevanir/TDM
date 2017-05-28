package DataManager;

import LoggingManager.Log;
import org.json.JSONObject;
import LoggingManager.MongoDB;

/**
 * Created by beep on 5/27/17.
 */
public class DataManager implements IDataManager {
    private MongoDB mongoDB;

    public DataManager() {
        mongoDB = new MongoDB();
    }

    public boolean writeData(String data) {
        JSONObject json = new JSONObject(data);
        if(mongoDB.insertData(json) == true) {
            System.out.println("Insertion to DB success");
            return true;
        }
        return false;
    }

    public void abort(String txnID) {
        mongoDB.removeData(txnID);
    }

    public boolean flushData(String data) {
        return false;
    }
}

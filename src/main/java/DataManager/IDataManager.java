package DataManager;

import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Created by beep on 5/21/17.
 */
public interface IDataManager {
    /*
    write logs given in the buffer to the database.
    @param: Array list of logsw
    @return true:  dumping in the database is successful
            false: unsuccessful
     */
    public boolean writeData(JSONObject data);

    /*
    Flushes log which is there in the buffer to the database
    @param: Array list of logs
    @return true: success
            false: failure
     */
    public boolean flushData(JSONObject data);
}
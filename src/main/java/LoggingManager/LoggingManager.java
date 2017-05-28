package LoggingManager;

import com.mongodb.DBCollection;
import org.json.JSONObject;

import java.util.ArrayList;

/**
 * Created by beep on 5/27/17.
 */
public class LoggingManager implements ILoggingManager{
    private MongoDB mongoDB;

    public LoggingManager() {
        mongoDB = new MongoDB();
    }

    public boolean writeLog(Log log) {
        JSONObject json = log.getJSON();
        if(mongoDB.insertLog(json) == true) {
            System.out.println("Insertion to DB success");
            return true;
        }
        return false;
    }

    public boolean flushLog(ArrayList<Log> logs) {
        return false;
    }
}

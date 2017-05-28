package LoggingManager;

import com.mongodb.*;
import com.mongodb.util.JSON;
import org.json.JSONObject;

import java.util.List;

/**
 * Created by beep on 5/27/17.
 */
public class MongoDB {
    private MongoClient mongoClient;
    private DBCollection collLog;
    private DBCollection collData;

    public MongoDB() {
        // To connect to mongodb server
        mongoClient = new MongoClient( "localhost" , 27017 );

        // Now connect to your databases
        DB db = mongoClient.getDB( "LogManager" );
        System.out.println("Connect to LogManager database successfully");

        //Get collection / table.
        collLog = db.getCollection("LogCollection");
        System.out.println("Collection logCollection selected successfully");

        // Now connect to your databases
        DB dbData = mongoClient.getDB( "DataManager" );
        System.out.println("Connect to DataManager database successfully");

        //Get collection / table.
        collData = dbData.getCollection("DataCollection");
        System.out.println("Collection DataManager selected successfully");
    }

    public boolean insertLog(JSONObject json) {
        DBObject dbObject = (DBObject) JSON.parse(json.toString());
        collLog.insert(dbObject);
        return true;
    }

    public boolean insertData(JSONObject json) {
        DBObject dbObject = (DBObject) JSON.parse(json.toString());
        collData.insert(dbObject);
        return true;
    }

    public void removeData(String txnID) {
        BasicDBObject query = new BasicDBObject();
        query.put("TID", txnID);
        collData.remove(query);
    }
}

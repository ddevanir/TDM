package LoggingManager;

import DataManager.DataManager;
import com.mongodb.*;
import com.mongodb.util.JSON;
import org.json.JSONArray;
import org.json.JSONObject;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.mongodb.client.model.Aggregates.count;

/**
 * Created by beep on 5/27/17.
 */
public class MongoDB {
    private MongoClient mongoClient;
    private DBCollection collLog;
    private DBCollection collData;
    private DBCollection collPendingTransactions;
    private DBCollection collFirstLsn;
    private DB dbLog;
    private DB dbData;

    public MongoDB() {
        // To connect to mongodb server
        mongoClient = new MongoClient( "localhost" , 27017 );

        // Now connect to your databases
        dbLog = mongoClient.getDB( "LogManager" );
        System.out.println("Connect to LogManager database successfully");

        //Get collection / table.
        collLog = dbLog.getCollection("LogCollection");
        System.out.println("Collection logCollection created successfully");

        // Now connect to your databases
        dbData = mongoClient.getDB( "DataManager" );
        System.out.println("Connect to DataManager database successfully");

        //Get collection / table.
        collData = dbData.getCollection("DataCollection");
        System.out.println("Collection DataManager created successfully");

        collPendingTransactions = dbLog.getCollection("PendingTransactions");
        System.out.println("Collection pending transactions created successfully");

        collFirstLsn = dbLog.getCollection("FirstLsnTransactions");
        System.out.println("Collection First LSN transactions created successfully");

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

    public boolean addNewData(String policyID) {
        JSONObject payload = getPayload(policyID);
        DBObject dbObject = (DBObject) JSON.parse(payload.toString());
        collData.insert(dbObject);
        return true;
    }

    public boolean updateData(JSONObject payload ) {
        DBObject dbObject = (DBObject) JSON.parse(payload.toString());
        String policyID = DataManager.getPolicyID(payload);
        BasicDBObject searchQuery = new BasicDBObject().append("policyID", policyID);

        collData.update(searchQuery, dbObject, true,false);
        return true;
    }
//
//    public boolean updateData(String payload) {
//        BasicDBObject query = new BasicDBObject();
//        String policyID = DataManager.getPolicyID(new JSONObject(payload));
//        query.put("payload", payload);
//
//        BasicDBObject searchQuery = new BasicDBObject().append("policyID", policyID);
//
//        collData.update(searchQuery, query);
//        return true;
//    }

    private JSONObject getPayload(String policyID) {
        BasicDBObject query = new BasicDBObject();
        BasicDBObject field = new BasicDBObject();
        field.put("payload.id", policyID);
        DBCursor cursor = collLog.find(query,field);
        JSONObject payload = null;

        while (cursor.hasNext()) {
            BasicDBObject obj = (BasicDBObject) cursor.next();

            payload = (JSONObject) cursor.curr().get("payload.id");
        }
        return payload;
    }

    public void removeData(String txnID) {
        BasicDBObject query = new BasicDBObject();
        query.put("TID", txnID);
        collData.remove(query);
    }

    public void insertPendingTransaction(String txnID) {
        JSONObject obj = new JSONObject();
        obj.put("TID",txnID);
        DBObject dbObject = (DBObject) JSON.parse(obj.toString());
        collPendingTransactions.insert(dbObject);
    }

    public void removePendingTid(String txnID) {
        BasicDBObject query = new BasicDBObject();
        query.put("TID", txnID);
        collPendingTransactions.remove(query);
    }

    public ArrayList<String> getPendingTransaction() {
        BasicDBObject query = new BasicDBObject();
        BasicDBObject field = new BasicDBObject();
        DBCursor cursor = dbLog.getCollection("PendingTransactions").find();
        ArrayList<String> arrayList = new ArrayList<String>();
        while (cursor.hasNext()) {
            BasicDBObject obj = (BasicDBObject) cursor.next();
            arrayList.add(obj.getString("TID"));
        }
        return arrayList;
    }

    public void insertFirstLsn(String txnID, Integer lsn) {
        JSONObject json = new JSONObject();
        json.put("TID", txnID);
        json.put("LSN", lsn);
        DBObject dbObject = (DBObject) JSON.parse(json.toString());
        collFirstLsn.insert(dbObject);
    }

    public void removeFirstLsn(Integer lsn) {
        BasicDBObject query = new BasicDBObject();
        query.put("LSN", lsn);
        // Delete All documents from collection Using blank BasicDBObject
        collFirstLsn.remove(query);
    }

    // Delete All documents from collection Using blank BasicDBObject
    public void removeFirstLsn() {
        BasicDBObject query = new BasicDBObject();
        collFirstLsn.remove(query);
    }


    public ArrayList<Integer> queryFirstLsn() {
        ArrayList<Integer > arrayList = new ArrayList<Integer>();
        DBCursor cursor = collFirstLsn.find();

        for(DBObject dbObject : cursor) {
            Integer lsn = (Integer) dbObject.get("LSN");
            arrayList.add(lsn);
        }
        return arrayList;
    }

    public JSONObject searchPayload(Integer lsn) {
        BasicDBObject query = new BasicDBObject();
        query.put("LSN", lsn);
        String str = null;
        Integer nextLsn = 0;
        DBCursor cursor = collLog.find(query);
        if (cursor.hasNext()) {
            BasicDBObject obj = (BasicDBObject) cursor.next();

            str = obj.getString("payload");
            nextLsn = obj.getInt("NextLSN");
        }
        JSONObject json = new JSONObject();
        json.put("payload", str);
        json.put("nextLsn", nextLsn);
        return json;
    }

    public JSONObject queryPayload(Integer lsn) {
        BasicDBObject query = new BasicDBObject();
        BasicDBObject field = new BasicDBObject();
        field.put("LSN", lsn);
        DBCursor cursor = collLog.find(query,field);
        JSONObject prevPayload = null;
        if (cursor.hasNext()) {
            BasicDBObject obj = (BasicDBObject) cursor.next();
            prevPayload = (JSONObject) cursor.curr().get("payload");
        }
        return prevPayload;
    }

    public JSONObject queryPayload(String policyID) {
        BasicDBObject query = new BasicDBObject();
        //BasicDBObject field = new BasicDBObject();
        query.put("payload.policyID", policyID);
        DBCursor cursor = collLog.find(query);
        ArrayList<String> arr = new ArrayList<String>();
        while (cursor.hasNext()) {
            BasicDBObject obj = (BasicDBObject) cursor.next();
            arr.add(obj.getString("payload"));
        }
        if(arr.size() >= 1) {
            String lastPayload = arr.get(arr.size() - 1);
//            JSONObject obj = new JSONObject();
//            obj.put("prevPayload",lastPayload);
//            DBObject dbObject = (DBObject) JSON.parse( obj.toString());
            return (new JSONObject(lastPayload));
        }
        return null;
    }

    public boolean isPolicyID(String policyID) {
        BasicDBObject query = new BasicDBObject();
        BasicDBObject field = new BasicDBObject();
        field.put("payload.id", policyID);
        DBCursor cursor = dbData.getCollection("DataCollection").find(query,field);
        String existingPolicyID = null;

        if (cursor.hasNext()) {
            BasicDBObject obj = (BasicDBObject) cursor.next();

            existingPolicyID = cursor.curr().get("payload.id").toString();
        }
        if(existingPolicyID == null) {
            return false;
        }
        return true;
    }

    public String getPolicyID(String lsn) {
        BasicDBObject query = new BasicDBObject();
        BasicDBObject field = new BasicDBObject();
        field.put("LSN", lsn);
        DBCursor cursor = dbLog.getCollection("LogCollection").find(query,field);
        String policyID = null;

        while (cursor.hasNext()) {
            BasicDBObject obj = (BasicDBObject) cursor.next();

            DBObject dbObject = (DBObject) cursor.curr().get("payload");
            policyID = (String) dbObject.get("id");
        }
        return policyID;
    }

    public void sortRecordDescendingOrder() {
        //dbLog.getCollection("LogCollection").find({}).sort({_id:-1}).limit(1);
        BasicDBObject sortObject = new BasicDBObject("_id", -1);
        collLog.find().sort(sortObject).limit(1);
    }

    public void updateDocTimeTraversal(Timestamp timestamp) {
        DBCursor cursor = collLog.find();

        if(cursor.hasNext()) {
            DBObject dbObject = (DBObject) cursor.curr().get("payload");
        }
    }

    public List<JSONObject> getTimeTraversalRecords(Timestamp ts){
        BasicDBObject query = new BasicDBObject("timestamp", new BasicDBObject("$gt", ts));
        DBCursor cursor = collLog.find(query);
        List<JSONObject> ret = new ArrayList<JSONObject>();
        while (cursor.hasNext()) {
            JSONObject json = new JSONObject(cursor.curr().toString());
            ret.add(json);
        }
        return ret;
    }

    public void deletePolicy(String policyID){
        BasicDBObject query = new BasicDBObject();
        query.put("policyID", policyID);
        collData.remove(query);
    }
}

package DataManager;

import LoggingManager.Log;
import org.json.JSONObject;
import LoggingManager.MongoDB;

import java.util.List;

/**
 * Created by beep on 5/27/17.
 */
public class DataManager implements IDataManager {
    private MongoDB mongoDB;

    public DataManager() {
        mongoDB = new MongoDB();
    }

    public boolean writeData(JSONObject data) {
        JSONObject json = new JSONObject(data);
        if(mongoDB.insertData(json) == true) {
            System.out.println("Insertion to DB success");
            return true;
        }
        return false;
    }

//    public boolean writeNewData(Data data) {
//        JSONObject json = data.getJSON();
//        if(mongoDB.insertData(json) == true) {
//            System.out.println("Insertion to DB Data is successfull");
//            return true;
//        }
//        return false;
//    }
//
//    public boolean addNewData(String policyID) {
//        if(mongoDB.addNewData(policyID) == true) {
//            System.out.println("Insertion to DB Data is successfull");
//            return true;
//        }
//        return false;
//    }

    public boolean updateData(Data data) {
        JSONObject json = data.getJSON();
        if(mongoDB.updateData(json) == true) {
            System.out.println("Update to the Data DB is successfull");
            return true;
        }
        return false;
    }

    public boolean updateData(String payload) {
        if(mongoDB.updateData(new JSONObject(payload)) == true) {
            System.out.println("Update to the Data DB is successfull");
            return true;
        }
        return false;
    }

    public boolean flushData(JSONObject data) {
        return false;
    }


    public Data createDataRecord(JSONObject payload, Data.dataType type) {
        Data data = new Data(payload, Data.dataType.PENDING);
        return data;
    }

//    public boolean PolicyIDPresent(Data data) {
//        JSONObject payload = data.getPayLoad();
//        String policyID = (String) payload.get("id");
//        if(mongoDB.isPolicyID(policyID) == true) {
//            return true;
//        } else {
//            return false;
//        }
//    }
//
//    public boolean isPolicyID(String policyID) {
//        if(mongoDB.isPolicyID(policyID) == true) {
//            return true;
//        } else {
//            return false;
//        }
//    }

    public static String getPolicyID(JSONObject payload ) {
        String policyID = (String) payload.get("policyID");
        return policyID;
    }

    public void deletePolicy(String policyID){
        mongoDB.deletePolicy(policyID);
    }

    public void deletePolicyFromOld(String policyID){
        mongoDB.deletePolicyfromOld(policyID);
    }

    public void addNewData(List<JSONObject> jsonList){ mongoDB.addNewData(jsonList);}

    public void updateNewData(JSONObject json ){ mongoDB.updateNewData(json);}

    public List<JSONObject> getRecords(long ts){
        mongoDB.dropOldPolicyCollection();
        return mongoDB.geDataRecords(ts);
    }
}

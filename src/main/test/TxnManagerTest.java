import TxnManager.TxnManager;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;

import java.sql.Timestamp;

/**
 * Created by beep on 5/27/17.
 */
public class TxnManagerTest {
    private TxnManager txnManager;

    @Before
    public void setUp() throws Exception {
        txnManager = new TxnManager();
    }

    @Test
    public void test1() throws Exception {
        String TID = txnManager.begin();
        JSONObject json1 = new JSONObject();
        json1.put("policyID", "1");
        json1.put("author", "Alice");
        txnManager.writePolicy(TID, json1);
        txnManager.commit(TID);

        String TID2 = txnManager.begin();
        JSONObject json2 = new JSONObject();
        json2.put("policyID", "2");
        json2.put("author", "Alice");
        txnManager.writePolicy(TID2, json2);
        txnManager.commit(TID2);

    }

    @Test
    public void test4() throws Exception {
//        String TID = txnManager.begin();
//        JSONObject json1 = new JSONObject();
//        json1.put("policyID", "1");
//        json1.put("author", "Alice");
//        txnManager.writePolicy(TID, json1);
//        txnManager.commit(TID);

        String TID2 = txnManager.begin();
        JSONObject json2 = new JSONObject();
        json2.put("policyID", "1");
        json2.put("author", "Deepthi");
        txnManager.writePolicy(TID2, json2);
        txnManager.commit(TID2);

    }

    @Test
    public void TestRecovery() throws Exception {
        String TID = txnManager.begin();
        JSONObject json1 = new JSONObject();
        json1.put("policyID", "1");
        json1.put("author", "Alice");
        txnManager.writePolicy(TID, json1);
        txnManager.commit(TID);
        txnManager.recovery();
    }

    @Test
    public void TestTimeTraversal() throws Exception {

        for(int i = 0; i < 3; i++) {
            String TID = txnManager.begin();
            JSONObject json = new JSONObject();
            json.put("policyID", String.valueOf(i));
            txnManager.writePolicy(TID, json);
            txnManager.commit(TID);
        }

        txnManager.flush();
        Thread.sleep(5*1000);
        java.sql.Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        Thread.sleep(5*1000);
        for(int i = 3; i < 6; i++) {
            String TID = txnManager.begin();
            JSONObject json = new JSONObject();
            json.put("policyID", String.valueOf(i));
            txnManager.writePolicy(TID, json);
            txnManager.commit(TID);
        }
        txnManager.timeTraversal(timestamp);
    }

    @Test
    public void test3() throws Exception {
//        String TID = txnManager.begin();
//        for(int i = 0; i < 6; i++) {
//            JSONObject json = new JSONObject();
//            json.put("id", i);
//            txnManager.writePolicy(TID, json.toString());
//        }
//        txnManager.abort(TID);
    }
}

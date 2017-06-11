import LoggingManager.MongoDB;
import TxnManager.TxnManager;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by beep on 5/27/17.
 */
public class TxnManagerTest {
    private TxnManager txnManager;
    private MongoDB mongoDB = new MongoDB();

    private String getCurTime(){
        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date date = new Date();
        return dateFormat.format(date);
    }
    @Before
    public void setUp() throws Exception {
        //mongoDB.dropAllDB();
        txnManager = new TxnManager(0);
    }

    @Test
    public void testCommit() throws Exception {
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
    public void testFlushBeforeCommit() throws Exception {
        String TID = txnManager.begin();
        JSONObject json1 = new JSONObject();
        json1.put("policyID", "1");
        json1.put("author", "Alice");
        txnManager.writePolicy(TID, json1);
        txnManager.flush();
        System.out.println("Sleep start in testFlushBeforeCommit");
        Thread.sleep(15*1000);
        System.out.println("Sleep start in testFlushBeforeCommit");

        txnManager.commit(TID);
    }

    @Test
    public void testAbort() throws Exception {
        String TID2 = txnManager.begin();
        JSONObject json2 = new JSONObject();
        json2.put("policyID", "1");
        json2.put("author", "Deepthi");
        txnManager.writePolicy(TID2, json2);
        txnManager.abort(TID2);
    }

    @Test
    public void TestRecovery() throws Exception {
        txnManager = new TxnManager(10*1000);
        String TID = txnManager.begin();
        JSONObject json1 = new JSONObject();
        json1.put("policyID", "1");
        json1.put("author", "Alice");
        txnManager.writePolicy(TID, json1);
        txnManager.commit(TID);
        System.out.println("Sleep start in TestRecovery");
        Thread.sleep(15*1000);
        System.out.println("Sleep start in TestRecovery");
        txnManager.recovery();
    }

    @Test
    public void TestTimeTraversalDeletion() throws Exception {

        for(int i = 0; i < 3; i++) {
            String TID = txnManager.begin();
            JSONObject json = new JSONObject();
            json.put("policyID", String.valueOf(i));
            json.put("author", "A"+ String.valueOf(i));
            txnManager.writePolicy(TID, json);
            txnManager.commit(TID);
        }

        txnManager.flush();
        Thread.sleep(5*1000);
        String timestamp = getCurTime();
        //long timestamp = System.currentTimeMillis();
        Thread.sleep(5*1000);
        for(int i = 3; i < 6; i++) {
            String TID = txnManager.begin();
            JSONObject json = new JSONObject();
            json.put("author", "A"+ String.valueOf(i));
            json.put("policyID", String.valueOf(i));
            txnManager.writePolicy(TID, json);
            txnManager.commit(TID);
        }
        txnManager.timeTraversal(timestamp);
    }

    @Test
    public void TestTimeTraversalUpdate() throws Exception {

        for(int i = 0; i < 3; i++) {
            String TID = txnManager.begin();
            JSONObject json = new JSONObject();
            json.put("policyID", String.valueOf(i));
            json.put("author", "A"+ String.valueOf(i));
            txnManager.writePolicy(TID, json);
            txnManager.commit(TID);
        }

        txnManager.flush();
        Thread.sleep(5*1000);
        String timestamp = getCurTime();
        //long timestamp = (System.currentTimeMillis());
        Thread.sleep(5*1000);

        for(int i = 0; i < 2; i++) {
            String TID = txnManager.begin();
            JSONObject json = new JSONObject();
            json.put("policyID", String.valueOf(i));
            json.put("author", "B"+ String.valueOf(i));
            txnManager.writePolicy(TID, json);
            txnManager.commit(TID);
        }
        System.out.println("Sleep start in TestTimeTraversalUpdate");
        Thread.sleep(10*1000);
        System.out.println("Sleep start in TestTimeTraversalUpdate");
        txnManager.timeTraversal(timestamp);
    }

    @Test
    public void TestTimeTraversalInBetween() throws Exception {
        String timestamp = null;
        for(int i = 0; i < 3; i++) {
            String TID = txnManager.begin();
            JSONObject json = new JSONObject();
            json.put("policyID", String.valueOf(i));
            if(i == 1) {
                Thread.sleep(5 * 1000);
                timestamp = getCurTime();
                 //timestamp= (System.currentTimeMillis());
                Thread.sleep(5 * 1000);
            }
            
            json.put("author", "A"+ String.valueOf(i));
            txnManager.writePolicy(TID, json);
            txnManager.commit(TID);
        }
        for(int i = 0; i < 2; i++) {
            String TID = txnManager.begin();
            JSONObject json = new JSONObject();
            json.put("policyID", String.valueOf(i));
            json.put("author", "B"+ String.valueOf(i));
            txnManager.writePolicy(TID, json);
            txnManager.commit(TID);
        }
        System.out.println("Sleep start in TestTimeTraversalInBetween");
        Thread.sleep(10*1000);
        System.out.println("Sleep start in TestTimeTraversalInBetween");
        txnManager.flush();
        txnManager.timeTraversal(timestamp);
    }

    // multiple time traversals
    @Test
    public void TestTimeTraversalMultiple() throws Exception {
        //long timestamp1 = 0;
        String timestamp1 = null;
        for(int i = 0; i < 3; i++) {
            String TID = txnManager.begin();
            JSONObject json = new JSONObject();
            json.put("policyID", String.valueOf(i));
            json.put("author", "A"+ String.valueOf(i));
            txnManager.writePolicy(TID, json);
            txnManager.commit(TID);
        }
        Thread.sleep(5*1000);
        timestamp1 = getCurTime();
        //timestamp1 = (System.currentTimeMillis());
        Thread.sleep(5*1000);
        for(int i = 0; i < 3; i++) {
            String TID = txnManager.begin();
            JSONObject json = new JSONObject();
            json.put("policyID", String.valueOf(i));
            json.put("author", "B"+ String.valueOf(i));
            txnManager.writePolicy(TID, json);
            txnManager.commit(TID);
        }
        Thread.sleep(5*1000);
        //long timestamp2 = (System.currentTimeMillis());
        String timestamp2 = getCurTime();
        Thread.sleep(5*1000);
        for(int i = 0; i < 3; i++) {
            String TID = txnManager.begin();
            JSONObject json = new JSONObject();
            json.put("policyID", String.valueOf(i));
            json.put("author", "C"+ String.valueOf(i));
            txnManager.writePolicy(TID, json);
            txnManager.commit(TID);
        }
        txnManager.flush();
        System.out.println("Sleep start in TestTimeTraversalMultiple");
        Thread.sleep(1*1000);
        System.out.println("Sleep start in TestTimeTraversalMultiple");
        txnManager.timeTraversal(timestamp2);
        System.out.println("Sleep start in TestTimeTraversalMultiple");
        Thread.sleep(1*1000);
        System.out.println("Sleep start in TestTimeTraversalMultiple");
        txnManager.timeTraversal(timestamp1);

    }
    @Test
    public void TestTxnAfterTimeTraversal() throws Exception {
        //long timestamp1 = 0;
        String timestamp1 = null;
        for(int i = 0; i < 3; i++) {
            String TID = txnManager.begin();
            JSONObject json = new JSONObject();
            json.put("policyID", String.valueOf(i));
            json.put("author", "A"+ String.valueOf(i));
            txnManager.writePolicy(TID, json);
            txnManager.commit(TID);
        }
        Thread.sleep(5*1000);
        timestamp1 = getCurTime();
        //timestamp1 = (System.currentTimeMillis());
        Thread.sleep(5*1000);
        for(int i = 3; i < 6; i++) {
            String TID = txnManager.begin();
            JSONObject json = new JSONObject();
            json.put("policyID", String.valueOf(i));
            json.put("author", "B"+ String.valueOf(i));
            txnManager.writePolicy(TID, json);
            txnManager.commit(TID);
        }
        Thread.sleep(5*1000);
        String timestamp2 = getCurTime();
        //long timestamp2 = (System.currentTimeMillis());
        Thread.sleep(5*1000);
        for(int i = 6; i < 9; i++) {
            String TID = txnManager.begin();
            JSONObject json = new JSONObject();
            json.put("policyID", String.valueOf(i));
            json.put("author", "C"+ String.valueOf(i));
            txnManager.writePolicy(TID, json);
            txnManager.commit(TID);
        }
        Thread.sleep(5*1000);
        long timestamp3 = (System.currentTimeMillis());
        Thread.sleep(5*1000);


        txnManager.timeTraversal(timestamp2);

        for(int i = 9; i < 12; i++) {
            String TID = txnManager.begin();
            JSONObject json = new JSONObject();
            json.put("policyID", String.valueOf(i));
            json.put("author", "D"+ String.valueOf(i));
            txnManager.writePolicy(TID, json);
            txnManager.commit(TID);
        }
        txnManager.flush();

        System.out.println("Sleep start in TestTimeTraversalMultiple");
        Thread.sleep(1*1000);
        System.out.println("Sleep start in TestTimeTraversalMultiple");
        txnManager.timeTraversal(timestamp1);

    }

    // same as previous but instead of inserting new, update the same policies
    @Test
    public void TestTxnAfterTimeTraversalWithUpdate() throws Exception {
        //long timestamp1 = 0;
        String timestamp1 = null;
        for(int i = 0; i < 3; i++) {
            String TID = txnManager.begin();
            JSONObject json = new JSONObject();
            json.put("policyID", String.valueOf(i));
            json.put("author", "A"+ String.valueOf(i));
            txnManager.writePolicy(TID, json);
            txnManager.commit(TID);
        }
        Thread.sleep(5*1000);
        timestamp1 = getCurTime();
        //timestamp1 = (System.currentTimeMillis());
        Thread.sleep(5*1000);
        for(int i = 0; i < 3; i++) {
            String TID = txnManager.begin();
            JSONObject json = new JSONObject();
            json.put("policyID", String.valueOf(i));
            json.put("author", "B"+ String.valueOf(i));
            txnManager.writePolicy(TID, json);
            txnManager.commit(TID);
        }
        Thread.sleep(5*1000);
        //long timestamp2 = (System.currentTimeMillis());
        String timestamp2 = getCurTime();
        Thread.sleep(5*1000);
        for(int i = 0; i < 3; i++) {
            String TID = txnManager.begin();
            JSONObject json = new JSONObject();
            json.put("policyID", String.valueOf(i));
            json.put("author", "C"+ String.valueOf(i));
            txnManager.writePolicy(TID, json);
            txnManager.commit(TID);
        }
        Thread.sleep(5*1000);
        //long timestamp3 = (System.currentTimeMillis());
        String timestamp3 = getCurTime();
        Thread.sleep(5*1000);


        txnManager.timeTraversal(timestamp2);

        for(int i = 0; i < 3; i++) {
            String TID = txnManager.begin();
            JSONObject json = new JSONObject();
            json.put("policyID", String.valueOf(i));
            json.put("author", "D"+ String.valueOf(i));
            txnManager.writePolicy(TID, json);
            txnManager.commit(TID);
        }
        txnManager.flush();

        System.out.println("Sleep start in TestTimeTraversalMultiple");
        Thread.sleep(1*1000);
        System.out.println("Sleep start in TestTimeTraversalMultiple");
        txnManager.timeTraversal(timestamp1);

    }

    @Test
    public void TestFlushMonitorThread() throws Exception {
        TxnManager txnManager = new TxnManager(30);
        for(int i = 0; i < 3; i++) {
            String TID = txnManager.begin();
            JSONObject json = new JSONObject();
            json.put("policyID", String.valueOf(i));
            txnManager.writePolicy(TID, json);
            txnManager.commit(TID);
        }
        System.out.println("Sleep start in TestFlushMonitorThread");
        Thread.sleep(45*1000);
        System.out.println("Sleep end in TestFlushMonitorThread");
    }

    @Test
    public void TestViewHistory() throws Exception {

        for(int i = 0; i < 3; i++) {
            String TID = txnManager.begin();
            JSONObject json = new JSONObject();
            json.put("policyID", String.valueOf(i));
            json.put("author", "A"+ String.valueOf(i));
            txnManager.writePolicy(TID, json);
            txnManager.commit(TID);
        }

        txnManager.flush();
        Thread.sleep(5*1000);
        String timestamp = getCurTime();
        //long timestamp = System.currentTimeMillis();
        Thread.sleep(5*1000);
        for(int i = 3; i < 6; i++) {
            String TID = txnManager.begin();
            JSONObject json = new JSONObject();
            json.put("author", "A"+ String.valueOf(i));
            json.put("policyID", String.valueOf(i));
            txnManager.writePolicy(TID, json);
            txnManager.commit(TID);
        }
        txnManager.viewHistory(timestamp);
    }

    @Test
    public void TestGoingForwardInTime() throws Exception {
        TestTimeTraversalDeletion();

        txnManager.timeTraversal(getCurTime());
    }

    @Test
    public void TestGoingForwardInTime2() throws Exception {


        String timestamp1 = null;
        for(int i = 0; i < 3; i++) {
            String TID = txnManager.begin();
            JSONObject json = new JSONObject();
            json.put("policyID", String.valueOf(i));
            json.put("author", "A"+ String.valueOf(i));
            txnManager.writePolicy(TID, json);
            txnManager.commit(TID);
        }
        Thread.sleep(5*1000);
        timestamp1 = getCurTime();
        //timestamp1 = (System.currentTimeMillis());
        Thread.sleep(5*1000);
        for(int i = 3; i < 6; i++) {
            String TID = txnManager.begin();
            JSONObject json = new JSONObject();
            json.put("policyID", String.valueOf(i));
            json.put("author", "B"+ String.valueOf(i));
            txnManager.writePolicy(TID, json);
            txnManager.commit(TID);
        }
        Thread.sleep(5*1000);
        //long timestamp2 = (System.currentTimeMillis());
        String timestamp2 = getCurTime();
        Thread.sleep(5*1000);
        for(int i = 6; i < 9; i++) {
            String TID = txnManager.begin();
            JSONObject json = new JSONObject();
            json.put("policyID", String.valueOf(i));
            json.put("author", "C"+ String.valueOf(i));
            txnManager.writePolicy(TID, json);
            txnManager.commit(TID);
        }
        Thread.sleep(5*1000);
        //long timestamp3 = (System.currentTimeMillis());
        String timestamp3 = getCurTime();
        Thread.sleep(5*1000);
        txnManager.flush();

        txnManager.timeTraversal(timestamp1);
        txnManager.timeTraversal(timestamp2);
    }
    @Test
    public void TestKill() throws Exception {

        TxnManager txnManager = new TxnManager(1000); // secs
        String TID = txnManager.begin();
        JSONObject json = new JSONObject();
        json.put("policyID", String.valueOf(1));
        json.put("author", "A" + String.valueOf(1));
        txnManager.writePolicy(TID, json);
        txnManager.commit(TID);

        String TID2 = txnManager.begin();
        JSONObject json2 = new JSONObject();
        json2.put("policyID", String.valueOf(2));
        json2.put("author", "B" + String.valueOf(2));
        txnManager.writePolicy(TID2, json2);

        int a=1;

        txnManager.commit(TID2);

    }
    @Test // comment drop DB in Setup @beforeClass
    public void TestRecover(){
        TxnManager txnManager = new TxnManager(0);

    }
}

import TxnManager.TxnManager;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;

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
        JSONObject json = new JSONObject();
        json.put("id", "1");
        json.put("Name", "Deepthi");
        txnManager.writeLogData(TID, json.toString());
        txnManager.commit(TID);
    }

    @Test
    public void test2() throws Exception {
        String TID = txnManager.begin();
        for(int i = 0; i < 6; i++) {
            JSONObject json = new JSONObject();
            json.put("id", i);
            txnManager.writeLogData(TID, json.toString());
        }
        Thread.sleep(10000);
        txnManager.commit(TID);
    }

    @Test
    public void test3() throws Exception {
        String TID = txnManager.begin();
        for(int i = 0; i < 6; i++) {
            JSONObject json = new JSONObject();
            json.put("id", i);
            txnManager.writeLogData(TID, json.toString());
        }
        txnManager.abort(TID);
    }
}

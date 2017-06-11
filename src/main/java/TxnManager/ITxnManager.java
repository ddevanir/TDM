package TxnManager;

import org.json.JSONObject;

import java.sql.Timestamp;
import java.text.ParseException;

/**
 * Created by beep on 5/21/17.
 */
public interface ITxnManager {
    // @return unique transaction id
    public String begin();

    /*write the data and log in the buffer*/
    public void writePolicy(String transID, JSONObject payload);

    /* Completion of commit */
    public void commit(String txnID);

    /* Completion of abort */
    public void abort(String txnID);

    /*Flushes the contents of data buffer into the DataBase */
    public void flush();

    /*Traverse to timestamp T */
    public void timeTraversal(String timestamp) throws ParseException;

    /*Traverse to timestamp T */
    public void viewHistory(String timestamp) throws ParseException;


}

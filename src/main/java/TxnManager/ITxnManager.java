package TxnManager;

/**
 * Created by beep on 5/21/17.
 */
public interface ITxnManager {
    /* @return unique transaction id*/
    public String begin();

    /* Completion of commit */
    public void commit(String txnID);

    /* Completion of abort */
    public void abort(String txnID);
}

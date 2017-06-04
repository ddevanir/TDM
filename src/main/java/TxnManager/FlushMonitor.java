package TxnManager;

import DataManager.Data;
import DataManager.DataManager;
import LoggingManager.LoggingManager;
import org.json.JSONObject;

/**
 * Created by beep on 6/4/17.
 */
public class FlushMonitor implements Runnable {

    private Integer flushtime;
    private LoggingManager loggingManager;
    private DataManager dataManager;

     public FlushMonitor(Integer flushtime, LoggingManager loggingManager,DataManager dataManager){
        this.flushtime = flushtime;
        this.loggingManager = loggingManager;
        this.dataManager = dataManager;
    }
    public void run() {
        try {
            Thread.sleep(this.flushtime * 1000);
            TxnManager.AcquireLocks();
            flush();
            TxnManager.ReleaseLocks();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    public void flush() {
         System.out.println("Flush starting from FlushMonitorThread");
        for(Data data : TxnManager.bufferData) {
            if(data.getType() == Data.dataType.COMMITTED) {
                //writePolicy(data);
                dataManager.updateData(data);
                loggingManager.removeFirstLsn();
                JSONObject payload = data.getPayLoad();
                String policyID = DataManager.getPolicyID(payload);
                TxnManager.policyLsnMap.remove(policyID);
            } else {
                continue;
            }
        }
        TxnManager.bufferData.clear();
        System.out.println("Flush done in FlushMonitorThread");
    }

}

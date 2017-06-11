# TDM
1. Download MongoDB and run the server while executing the project. 
2. Also download RoboMongo to see the interface of the Log DB and Policy DB.

3. Tables
● Log DB
1. Pending TID - Keeps track of ongoing transactions.
2. <TID, FirstLSN> - Keeps track of first LSN of every Txn.
● Policy DB
1. CurrentPolicyDB
2. OldPolicyDB - is a queryable interface for view history.

4. Data Structures Used

● Log Buffer - Maintains buffer of logs which is flushed to the database on
commit/abort.
● Data Buffer - Maintains buffer of data which is flushed to the database on any
of the following:
○ Commit
○ Time based
○ On demand (API provided)
● PolicyID -> List of LSN

5. Features
1) Transaction - Begin, Write, Commit/Abort, Flush
2) Recovery - UNDO/REDO
3) View state of DB at given timestamp
4) Multiple Backward Time traversal*
5) Multiple Forward Time Traversal*
* - Assumption: We don’t allow new transactions after time traversal has occurred.


6. Transaction Manager Interface is as follows.

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

public void timeTraversal(Timestamp timestamp);

/*Traverse to timestamp T */

public void viewHistory(String timestamp) throws ParseException;

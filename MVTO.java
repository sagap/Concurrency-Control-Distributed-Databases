//package mvto;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *  implement a (main-memory) data store with MVTO.
 *  objects are <int, int> key-value pairs.
 *  if an operation is to be refused by the MVTO protocol,
 *  undo its xact (what work does this take?) and throw an exception.
 *  garbage collection of versions is not required.
 *  Throw exceptions when necessary, such as when we try to execute an operation in
 *  a transaction that is not running; when we insert an object with an existing
 *  key; when we try to read or write a nonexisting key, etc.
 *  Keep the interface, we want to test automatically!
 *
 **/


public class MVTO {
  /* TODO -- your versioned key-value store data structure */  
    private static int max_xact = 0;
    private static HashMap<Integer,Transaction>  transactions = new HashMap<>();            // hold transactionId - Transaction
    private static HashMap<Integer, ArrayList<Version>> versions_track = new HashMap<>(); // hold key - with all Versions
    private static Set<Integer> pendingCommits = new HashSet<>();
    
// returns transaction id == logical start timestamp
  public static int begin_transaction() { 
    // You might add code here!
    Transaction trans = new Transaction(++max_xact);
    transactions.put(max_xact, trans);
    System.out.println("BEGIN tnxId: "+max_xact);
    return max_xact;
  }

  // create and initialize new object in transaction xact
  public static void insert(int xact, int key, int value) throws Exception
  {
        System.out.println("INSERT "+ key+" value: "+value);
        if(versions_track.containsKey(key))
            throw new Exception("Key Already exists!");
        Version version = new Version(xact, key, value);
        ArrayList<Version> versions = new ArrayList<>();
        versions.add(version);
        versions_track.put(key, versions);
        transactions.get(xact).getWriteOps().add(0,version);
  }

  // return value of object key in transaction xact
  public static int read(int xact, int key) throws Exception
  {
        System.out.println("READ "+xact);
        if (!versions_track.containsKey(key))
            throw new Exception("Key does not exist");
        Transaction tr = transactions.get(xact);
        if(tr == null)
            throw new Exception("Error");
        if(tr.isAborted() || tr.isCommitted())
            throw new Exception("Transaction is already committed/aborted");
        Version lastVersion = findVersionWithLargestTS(xact, key);
        if(lastVersion != null)
        {
            if(tr.getTS() >= lastVersion.getReadTS())
            {
                lastVersion.setReadTS(tr.getTS());
                tr.getReadOps().add(lastVersion);
                findTnxsToWait(xact, key);
            }            
            System.out.println("Reads value: "+lastVersion.getValue());
            return lastVersion.getValue();
        }
        else
            throw new Exception("No LastVersion Exists! "+xact);   
  }

  // write value of existing object identified by key in transaction xact
  public static void write(int xact, int key, int value) throws Exception
  {
    System.out.println("WRITE "+xact + " key "+key + " value "+value);
    if (!versions_track.containsKey(key))
        throw new Exception("Key does not exist");
    Transaction tr = transactions.get(xact);
    if(tr == null)
        throw new Exception("Error");
    if(tr.isAborted() || tr.isCommitted())
        throw new Exception("Transaction is already committed/aborted");
    Version lastVersion = findVersionWithLargestTS(xact, key);
    if(lastVersion != null)
    {
        if(lastVersion.getReadTS() > tr.getTS())
        {
            rollback(xact);
            throw new Exception("Version has largest writeTS than transaction's TS"+lastVersion);
        }
        else if(lastVersion.getReadTS() <= tr.getTS()  && lastVersion.getWriteTS() == tr.getTS())
            lastVersion.setValue(value);
        else if(lastVersion.getReadTS() <= tr.getTS()  && lastVersion.getWriteTS() < tr.getTS())
        {
            Version version = new Version(xact, key, value);
            version.setReadTS(tr.getTS());
            version.setWriteTS(tr.getTS());
            versions_track.get(key).add(0, version);
            tr.getWriteOps().add(0,version);
        }
        System.out.println(transactions.get(xact));
    }
    else
    {
        throw new Exception("");
    }
    
  }

  public static void commit(int xact)   throws Exception { 
    System.out.println("COMMIT "+xact);
    Transaction tr = transactions.get(xact);
    if(tr == null)
        throw new Exception("Error");
//    System.out.println("Ab: "+tr.isAborted()+" Commit: "+tr.isCommitted());
    if(tr.isAborted() || tr.isCommitted())
        throw new Exception("Transaction is already committed/aborted" +xact);
         
    //check if has to wait for others txns to commit
    
    if(allowedToCommit(xact))
    {
        for (Version v : transactions.get(xact).getWriteOps())
            {
                System.out.println("V: "+v.getKey()+" "+v.getValue());
                v.setCommitted();
            }
        transactions.get(xact).setCommitted(true);
        System.out.println("Commit: "+transactions.get(xact));
        
        //check if pending commit can commit
        checkIfOthersCanCommit();
    }
    else
    {
        transactions.get(xact).setPendingCommit(true);
        pendingCommits.add(xact);
    }
    
    
  }

  public static void rollback(int xact) throws Exception {
      int TSRemoved = transactions.get(xact).getTS();
      HashMap<Integer,Integer> keysToBeRemoved = new HashMap<>();
      System.out.println("ABORT: "+xact);
      Transaction tr = transactions.get(xact);
      if(tr == null)
        throw new Exception("Error");
      if(tr.isAborted() || tr.isCommitted())
        throw new Exception("Transaction is already committed/aborted");
      for(Version v : transactions.get(xact).getWriteOps())
      { 
            if(!v.isCommitted())
                keysToBeRemoved.put(v.getKey(), -1);
      }
      for(Integer keyRemove : keysToBeRemoved.keySet())
      {
          for(int i=0;i<versions_track.get(keyRemove).size();i++)
              if(versions_track.get(keyRemove).get(i).getTransactionID() == xact)    // find the first time xact Wrote for this key And remove for then on
                keysToBeRemoved.put(keyRemove,i);
      }
      System.out.print(keysToBeRemoved);
      transactions.get(xact).getWriteOps().clear();
      transactions.get(xact).getReadOps().clear();
      transactions.get(xact).setCommitted(false);
      transactions.get(xact).setAborted(true);
      transactions.get(xact).setPendingCommit(false);
      abortWrites(keysToBeRemoved, xact);
      for(Integer key : keysToBeRemoved.keySet())
      {
          for(int i=0;i<versions_track.get(key).size();i++)
          {
              if(!versions_track.get(key).get(i).isCommitted() && versions_track.get(key).get(i).getTransactionID()==xact)
                  versions_track.get(key).remove(i);
          }      
      }
      System.out.println("WWWWWWWWWWWWWW"+transactions.get(xact));
      System.out.println(transactions);
      System.out.println("------------");
      System.out.println(versions_track);
//      Set<Integer> readsToRollBack = new HashSet<>();
//      for(Version v: transactions.get(xact).getReadOps())
//      {
//          readsToRollBack.add(v.getKey());
//      }
//      for(Integer key: readsToRollBack)
//      {
//          findLargestTimestampforVersion(key, TSRemoved);
//      }
  }
  
  //for cascade aborts
  // remove from pos until 0 
  public static void abortWrites(HashMap<Integer, Integer> keyPos, int selftxn)
  {
        try {
            Set<Integer> txns = new HashSet<>();
            for(Integer key : keyPos.keySet())
                for (int i = keyPos.get(key) ; i >= 0;i--)
                    if (!versions_track.get(key).get(i).isCommitted())
                    {
                        System.out.println("ABBBBB: "+transactions.get(versions_track.get(key).get(i).getTransactionID()));
                        txns.add(versions_track.get(key).get(i).getTransactionID());
//                 transactions.get(versions_track.get(key).get(i).getTransactionID()).setAborted(true);
//                 transactions.get(versions_track.get(key).get(i).getTransactionID()).setPendingCommit(false);
//                 versions_track.get(key).remove(i);
                    }
            for(Integer aborts :txns)
                if(!transactions.get(aborts).isAborted())
                    rollback(aborts);
        } catch (Exception ex) {
            Logger.getLogger(ex+"Avoid Exception!");
        }
  }
  
  
  public void cascadeAbort()
  {
  
  
  }
    
    public static Version findVersionWithLargestTS(int xact, int key) {
        /**
         * Used by write()
         * Finds the newest (most recent) version for a particular transaction, if any exists
         */
        Version toReturn = null;
        int max = -1;
    	ArrayList<Version> pastVersions = versions_track.get(key);
    	for (Version version : pastVersions) {
            if(version.getWriteTS() > max && version.getWriteTS()<= transactions.get(xact).getTS())    
            {
                max = version.getWriteTS();
                toReturn = version;
            }
        }
        return toReturn;
    }
    
    public static int findLargestTimestampforVersion(int key, int TSRemoved)
    {   
        int max = 0;
        for(Version v : versions_track.get(key))
        {
            if(v.getReadTS() != TSRemoved)
                if(v.getReadTS() > max)
                    max = v.getReadTS();
        }
        return max;
    }
  
    public static boolean checkForUncommittedWrites(int xact)
    {
        Transaction tr = transactions.get(xact);
        ArrayList<Version> tempV = tr.getReadOps();
        for (Transaction trans : transactions.values())
        {
            if(!trans.isCommitted() && trans.getTS() < trans.getTS())
            {
                Iterator it = tempV.iterator();
                while(it.hasNext())
                {
                    Version temp = (Version) it.next();
                    if(trans.getWriteOps().contains(temp))
                        return false;
                }
            }
        }
        return true;
    }
    
    // find for which txns it has to wait to commit
    public static void findTnxsToWait(int txn, int key)
    {
        for(Version v: versions_track.get(key))
        {
            if(v.getTransactionID()!=txn && !transactions.get(v.getTransactionID()).isCommitted() && transactions.get(txn).getTS() >= v.getReadTS())
            {
                System.out.println("BAzei: "+v.getTransactionID());
                transactions.get(txn).getWaitsToCommit().add(v.getTransactionID());
        
            }
        }
    }
    
    public static boolean allowedToCommit(int txn)
    {
        System.out.println("Not Allowed to Commit "+txn);
        boolean flag = true;
        for(Integer txns: transactions.get(txn).getWaitsToCommit())
        {
            if(!transactions.get(txns).isCommitted())
                flag = false;
        }
        return flag;
    }
    
    // check if pending commit proccesses can commit
    public static void checkIfOthersCanCommit()
    {
        for(Integer txns: pendingCommits)
            try {
                if(!transactions.get(txns).isCommitted())
                {
                    commit(txns);
                    pendingCommits.remove(txns);
                    transactions.get(txns).setPendingCommit(false);
                }
            } catch (Exception ex) {
                System.out.println(ex+" Catch exception to avoid throw");
            }
    }
}

class Transaction {
    
    private int txnId; // transaction ID
    private int TS; // start timestamp
    private boolean aborted; // check if the transaction is aborted
    private boolean committed; // check if the transaction is committed
    private ArrayList<Version> readOps;   // keeps track of Reads for a txn
    private ArrayList<Version> writeOps;   // keeps track of Writes for a txn
    private Set<Integer> waitsToCommit;   // waits for them to commit
    private boolean pendingCommit;
    
    public Transaction(int transactionID) {
        this.txnId = transactionID;
        this.TS = transactionID;
        this.aborted = false;
        this.committed = false;
        readOps = new ArrayList<>();
        writeOps = new ArrayList<>();       
        waitsToCommit = new HashSet<>();
        this.pendingCommit = false;
    }

    public int getTransactionID() {
        return txnId;
    }

    public void setTransactionID(int transactionID) {
        this.txnId = transactionID;
    }

    public int getTS() {
        return TS;
    }

    public void setTS(int startTS) {
        this.TS = startTS;
    }

    public boolean isAborted() {
        return aborted;
    }

    public void setAborted(boolean aborted) {
        this.aborted = aborted;
    }

    public boolean isCommitted() {
        return committed;
    }

    public void setCommitted(boolean committed) {
        this.committed = committed;
    }

    public void setTxnId(int txnId) {
        this.txnId = txnId;
    }

    public ArrayList<Version> getReadOps() {
        return readOps;
    }

    public void setReadOps(ArrayList<Version> readOps) {
        this.readOps = readOps;
    }

    public ArrayList<Version> getWriteOps() {
        return writeOps;
    }

    public void setWriteOps(ArrayList<Version> writeOps) {
        this.writeOps = writeOps;
    }

    public Set<Integer> getWaitsToCommit() {
        return waitsToCommit;
    }

    public void setWaitsToCommit(Set<Integer> waitsToCommit) {
        this.waitsToCommit = waitsToCommit;
    }

    public boolean isPendingCommit() {
        return pendingCommit;
    }

    public void setPendingCommit(boolean pendingCommit) {
        this.pendingCommit = pendingCommit;
    }

    @Override
    public String toString() {
        return "Transaction{" + "txnId=" + txnId + ", TS=" + TS + ", aborted=" + aborted + ", committed=" + committed + ", readOps=" + readOps + ", writeOps=" + writeOps + '}';
    }
    
}

class Version {
    private int txnID;
    private int key;
    private int value; 
    private int writeTS;
    private int readTS;
    private boolean committed; 
    //private ArrayList<Version> previousVersions;

    public Version(int txnID, int key, int value) {
    	this.txnID = txnID;
    	this.key = key;
    	this.value = value;
//    	this.previousVersions = pV;
        this.committed = false;
        this.writeTS = 0;
        this.readTS = 0;
    }
    
    public int getTransactionID() {
    	return this.txnID;
    }
    
    public void setValue(int value) {
    	this.value = value;
    }
    
    public int getValue() {
    	return this.value;
    }
    
    public int getKey() {
    	return this.key;
    }

    public void setCommitted() {
        this.committed = true;
    }
    
    public boolean isCommitted() {
        return this.committed;
    }

    public int getReadTS() {
        return this.readTS;
    }

    public void setTxnID(int txnID) {
        this.txnID = txnID;
    }   
    
    public void setWriteTS(int wTS) {
    	this.writeTS = wTS;
    }
    public void setReadTS(int readTS) {
    	this.readTS = readTS;
    }
     
    public int getWriteTS() {
        return this.writeTS;
    }

    @Override
    public String toString() {
        return "Version{" + "txnID=" + txnID + ", key=" + key + ", value=" + value + ", writeTS=" + writeTS + ", readTS=" + readTS + ", committed=" + committed + '}';
    }
    
    
}
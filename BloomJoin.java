
import java.io.*;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

public class BloomJoin {
    public static void main(String[] args) {
        ArrayList<R> r = new ArrayList<>();
        ArrayList<S> s = new ArrayList<>();
        for(int i = 0; i<10; i++) {
            r.add(new R(i * 2, i * 3));
//            s.add(new S(i * 3, i * 4));
        }
        for(int i = 0; i< 1000000; i++) {
            s.add(new S(i * 3, i * 5));
        }
        NodeA n1 = new NodeA(r);
        Node n2 = new NodeB(s);
        n1.run(n2);
        System.out.println("Size: "+n1.getResult().size());
        for(RS rs: n1.getResult()) {
            System.out.println(rs);
        }
        System.out.println(n1.getTotalMessageSize());
        System.out.println(n2.getTotalMessageSize());
    }
}

abstract class Node {
    public static final int BUCKETS = 10000;
    private long totalMessageSize;
    private Message inbox;

    /**
     * @param receiver the receiver node.
     * @param msg the message to be sent to the receiver node.
     */
    protected void send(Node receiver, Object msg) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(msg);
            oos.close();
            Message message = new Message(this, receiver, baos.toByteArray());
            totalMessageSize += baos.size();
            receiver.setInbox(message);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    protected void setInbox(Message msg) {
        this.inbox = msg;
    }

    /**
     * @return the last message that delivered to the current node.
     */
    protected Message getInbox() {
        return inbox;
    }

    /**
     * @return the total number of bytes that is sent from the current node.
     */
    public long getTotalMessageSize() {
        return totalMessageSize;
    }
    /**
     * The hash function that should be used by BloomJoin
     */
    public int hashFunction(int i) {
        return i % BUCKETS;
    }
    public abstract void run(Node other);
}

class NodeA extends Node {
    private List<R> data;
    private List<RS> result;

    public NodeA(List<R> data) {
        this.data = data;
    }

    public void run(Node other) {
        BitSet bloomFilter = new BitSet();   
        for(R temp : this.data)
            bloomFilter.set(this.hashFunction(temp.getB()),true);       // create Bloomfilter with the hashfunction
        this.send(other, bloomFilter);            // execute send() so that NodeB gets the BF
        other.run(this);                        // NodeB executes run 
        Object tempRelevantColumns = this.getInbox().getContent();    // get relevant cols sent by NodeB
        List<S> relevantColumns = (List) tempRelevantColumns;
        this.result = new ArrayList<>();
        for(S tempS : relevantColumns)            // join over the relevant columns
            for(R tempR : this.data)
                if(tempS.getB() == tempR.getB())
                    result.add(new RS(tempR.getA(),tempS.getB(),tempS.getC()));
    }

    /**
     * @return the result of BloomJoin.
     */
    public List<RS> getResult() {
        return result;
    }
}
class NodeB extends Node {
    private List<S> data;
    public NodeB(List<S> data) {
        this.data = data;
    }
    public void run(Node other) {
       Object tempMessage = this.getInbox().getContent();          // get the BF 
       List<S> relevantColumns = new ArrayList<>();
       BitSet bloomFilter = (BitSet) tempMessage;
       for (S temp : this.data)                                     // iterate over the B cols of S
            if(bloomFilter.get(this.hashFunction(temp.getB())))             // filter columns 
                relevantColumns.add(temp);            
       this.send(other, relevantColumns);                 // send back to Node A the relecant columns, S is serializable
    }
}

class R {
    private int a, b;
    public R(int a, int b) {
        this.a = a;
        this.b = b;
    }

    public int getA() {
        return a;
    }

    public int getB() {
        return b;
    }
}

class S implements Serializable {
    private int b, c;
    public S(int b, int c) {
        this.b = b;
        this.c = c;
    }

    public int getB() {
        return b;
    }

    public int getC() {
        return c;
    }
}

class RS {
    private int a, b, c;
    public RS(int a, int b, int c) {
        this.a = a;
        this.b = b;
        this.c = c;
    }

    public int getA() {
        return a;
    }

    public int getB() {
        return b;
    }

    public int getC() {
        return c;
    }

    @Override
    public String toString() {
        return "<" + a + ", " + b + ", " + c + ">";
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof RS) {
            RS rs = (RS) obj;
            return a == rs.a && b == rs.b && c == rs.c;
        } else {
            return false;
        }
    }
}


class Message {
    private Node sender;
    private Node receiver;
    private byte[] msg;
    public Message(Node sender, Node receiver, byte[] msg) {
        this.sender = sender;
        this.receiver = receiver;
        this.msg = msg;
    }

    /**
     * @return the object that the message contains.
     */
    public Object getContent() {
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(msg);
            ObjectInputStream ois = new ObjectInputStream(bis);
            return ois.readObject();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
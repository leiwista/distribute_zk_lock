package com.kevinlong.distribute_lock.slock;

import org.apache.zookeeper.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * Created by kl0297 on 2019/5/13.
 */
public class ShareWriteLockTest implements Watcher {

    public final static String LOCK_ROOT_PATH = "/slock";

    public static CountDownLatch connectCountDown = new CountDownLatch(1);
    public static CountDownLatch lockCountDown = new CountDownLatch(1);

    public static void main(String[] args) throws Exception {
        ShareWriteLockTest test = new ShareWriteLockTest();
        test.init();
        test.tryLock();
    }

    public static class LockNode {
        public String lockType;
        public int nodeSeq;

        public LockNode(String lockType, int nodeSeq) {
            this.lockType = lockType;
            this.nodeSeq = nodeSeq;
        }
    }

    public ZooKeeper zookeeper;
    public LockNode lockNode;

    public ShareWriteLockTest() {
    }

    public ShareWriteLockTest init() throws Exception {
        zookeeper = new ZooKeeper("127.0.0.1:2181", 5000, this);
        connectCountDown.await();
        return this;
    }

    public void tryLock() throws Exception {
        String myNode = this.zookeeper.create(LOCK_ROOT_PATH+"/"+"writer-",
                "".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL_SEQUENTIAL);
        this.lockNode = this.getLockNodeFromPath(myNode);
        System.out.println("create lock node: " + myNode);

        // check pre nodes
        List<String> locks = this.zookeeper.getChildren(LOCK_ROOT_PATH, this);

        if (!this.canLock(locks)) {
            lockCountDown.await();
        }

        // main exit and ephemeral node delete automatic
        System.out.println("get the writer lock for node: " + String.valueOf(this.lockNode.nodeSeq));
        int i = 0;
        while (i < 5) {
            System.out.println("...");
            Thread.sleep(1000);
            i++;
        }
    }

    public LockNode getLockNodeFromPath(String nodePath) {
        String nodename = nodePath.substring(nodePath.lastIndexOf("/") + 1);
        String[] nodeParts = nodename.split("-");
        return new LockNode(nodeParts[0], Integer.valueOf(nodeParts[1]));
    }

    public boolean canLock(List<String> lockNodes) {
        List<LockNode> allNodes = new ArrayList<>();
        for (String nodeName : lockNodes) {
            allNodes.add(this.getLockNodeFromPath(nodeName));
        }
        allNodes.sort(((o1, o2) -> o1.nodeSeq < o2.nodeSeq ? -1 : 1));

        return allNodes.get(0).nodeSeq == this.lockNode.nodeSeq &&
                allNodes.get(0).lockType.equals(this.lockNode.lockType);
    }

    @Override
    public void process(WatchedEvent event) {
        System.out.println("Receive watched event:" + event);
        if(event.getState() == Event.KeeperState.SyncConnected){
            if (event.getType() == Event.EventType.None) {
                connectCountDown.countDown();
            }else if(event.getType() == Event.EventType.NodeChildrenChanged) {
                if (event.getPath().equals("/slock")) {
                    System.out.println("Node Children changed for: " + event.getPath());
                    try {
                        List<String> locks = this.zookeeper.getChildren(LOCK_ROOT_PATH, this);
                        if (this.canLock(locks)) {
                            lockCountDown.countDown();
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }
}

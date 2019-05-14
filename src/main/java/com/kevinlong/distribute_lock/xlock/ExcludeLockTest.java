package com.kevinlong.distribute_lock.xlock;

import org.apache.zookeeper.*;

import java.util.concurrent.CountDownLatch;

/**
 * Created by kl0297 on 2019/5/9.
 */
public class ExcludeLockTest implements Watcher {

    private static ZooKeeper zookeeper;
    private static CountDownLatch countDownLatch = new CountDownLatch(1);

    public static void main(String[] args) throws Exception {
        String processName = args[0];
        zookeeper = new ZooKeeper("127.0.0.1:2181", 5000, new ExcludeLockTest(processName));
        countDownLatch.await();
    }

    private String name;
    public ExcludeLockTest(String name) {
        this.name = name;
    }

    private void getLock() {
        try {
            String path = zookeeper.create("/xlock", this.name.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            System.out.println("success get the exclude lock:" + path);
            Thread.sleep(5000);

            System.out.println("process " + this.name + " exits and releases the exclude lock.");
            zookeeper.close();
            countDownLatch.countDown();
        } catch (KeeperException e) {
            if (e instanceof KeeperException.NodeExistsException) {
                System.out.println("The exclude lock exists. please wait.");
                zookeeper.getData("/xlock", this, null, null);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void process(WatchedEvent event) {
        System.out.println("Receive watched event:" + event);
        if(event.getState() == Event.KeeperState.SyncConnected){
            if (event.getType() == Event.EventType.None) {
                getLock();
            } else if (event.getType() == Event.EventType.NodeDeleted) {
                if (event.getPath().equals("/xlock")) {
                    System.out.println("path /xlock deleted.");
                    getLock();
                }
            }
        }
    }
}

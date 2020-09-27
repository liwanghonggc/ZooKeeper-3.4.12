package org.apache.zookeeper.my.lock;

import com.google.common.collect.Iterables;
import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * @author lwh
 * @date 2020-09-26
 * @desp
 */
public class DistributedLock implements Lock, Watcher {

    private ZooKeeper zooKeeper;

    private String parentPath;

    private CountDownLatch latch = new CountDownLatch(1);

    private static ThreadLocal<String> currentNodePath = new ThreadLocal<>();

    public DistributedLock(String url, int sessionTimeout, String parentPath) {
        this.parentPath = parentPath;
        try {
            zooKeeper = new ZooKeeper(url, sessionTimeout, this);
            latch.await();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void process(WatchedEvent event) {
        System.out.println("收到事件: " + event);

        if (event.getType() == Event.EventType.None && event.getState() == Event.KeeperState.SyncConnected) {
            System.out.println("已经连接成功");
            latch.countDown();
        }
    }

    @Override
    public void lock() {
        if (tryLock()) {
            System.out.println(Thread.currentThread().getName() + "成功获取锁");
        } else {
            String myPath = currentNodePath.get();
            synchronized (myPath) {
                try {
                    myPath.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            System.out.println(Thread.currentThread().getName() + "等待锁完成");

            // wait被唤醒之后, 再次尝试获取锁
            lock();
        }
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {

    }

    @Override
    public boolean tryLock() {
        String myPath = currentNodePath.get();

        try {
            if (myPath == null) {
                myPath = zooKeeper.create(parentPath + "/", "dis_lock".getBytes(),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
                currentNodePath.set(myPath);
                System.out.println(Thread.currentThread().getName() + "已经创建" + myPath);
            }

            final String currentPath = myPath;
            List<String> allNodes = zooKeeper.getChildren(parentPath, false);
            Collections.sort(allNodes);

            // 判断创建的节点是不是最小的节点, 如果是说明获取锁成功了
            String nodeName = currentPath.substring((parentPath + "/").length());
            if (allNodes.get(0).equals(nodeName)) {
                System.out.println(Thread.currentThread().getName() + "tryLock成功");
                return true;
            } else {
                // 不是最小的节点, 需要利用ZooKeeper监听机制监听节点被删除, 然后重新尝试获取锁
                String targetNodeName = parentPath + "/" + allNodes.get(0);

                for (String node : allNodes) {
                    if (nodeName.equals(node)) {
                        break;
                    } else {
                        targetNodeName = node;
                    }
                }

                // targetNodeName记录的是比我小的上一个节点, 当我上一个节点被删除后, 就轮到我获取锁了
                targetNodeName = parentPath + "/" + targetNodeName;

                System.out.println(Thread.currentThread().getName() + "需要等待节点被删除" + targetNodeName);

                zooKeeper.exists(targetNodeName, new Watcher() {
                    @Override
                    public void process(WatchedEvent event) {
                        System.out.println("收到事件:" + event);

                        if (event.getType() == Event.EventType.NodeDeleted) {
                            synchronized (currentPath) {
                                currentPath.notify();
                            }

                            System.out.println(Thread.currentThread().getName() + "获取到NodeDeleted通知, 重新尝试获取锁");
                        }
                    }
                });
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        return false;
    }

    @Override
    public void unlock() {
        String myPath = currentNodePath.get();
        if (myPath != null) {
            System.out.println(Thread.currentThread().getName() + "释放锁");
            try {
                zooKeeper.delete(myPath, -1);
                currentNodePath.remove();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public Condition newCondition() {
        return null;
    }

}

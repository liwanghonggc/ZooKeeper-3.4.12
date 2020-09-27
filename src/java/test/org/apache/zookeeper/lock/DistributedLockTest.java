package org.apache.zookeeper.lock;

/**
 * @author lwh
 * @date 2020-09-26
 * @desp
 */
public class DistributedLockTest {

    public static void main(String[] args) {
        for (int i = 0; i < 10; i++) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    DistributedLock distributedLock = new DistributedLock("localhost:2181", 3000, "/lwh");
                    try {
                        distributedLock.lock();
                        System.out.println(Thread.currentThread().getName() + "开始处理");
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } finally {
                        distributedLock.unlock();
                    }
                }
            }).start();
        }
    }
}

package org.apache.zookeeper.my.client;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;

/**
 * @author lwh
 * @date 2020-09-27
 * @desp
 */
public class ClientDemo implements Watcher {

    private static Stat stat = new Stat();

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        ZooKeeper zooKeeper = new ZooKeeper("localhost:2181", 10000, new ClientDemo());


        byte[] resByte = zooKeeper.getData("/dubbo", new ClientDemo(), stat);

        if (resByte.length == 0) {
            System.out.println("no data");
        } else {
            String result = new String(resByte);
            System.out.println("/dubbo: " + result);
        }

        while (true) {
            Thread.sleep(3000);
        }
    }

    @Override
    public void process(WatchedEvent event) {
        System.out.println("接收到Watch通知: " + event);
    }
}

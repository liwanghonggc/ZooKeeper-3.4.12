/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.server;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import javax.management.JMException;

import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.jmx.ManagedUtil;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;

/**
 * This class starts and runs a standalone ZooKeeperServer.
 */
@InterfaceAudience.Public
public class ZooKeeperServerMain {
    private static final Logger LOG =
        LoggerFactory.getLogger(ZooKeeperServerMain.class);

    private static final String USAGE =
        "Usage: ZooKeeperServerMain configfile | port datadir [ticktime] [maxcnxns]";

    private ServerCnxnFactory cnxnFactory;

    /*
     * Start up the ZooKeeper server.
     *
     * @param args the configfile or the port datadir [ticktime]
     */
    public static void main(String[] args) {
        ZooKeeperServerMain main = new ZooKeeperServerMain();
        try {
            // 会话
            main.initializeAndRun(args);
        } catch (IllegalArgumentException e) {
            LOG.error("Invalid arguments, exiting abnormally", e);
            LOG.info(USAGE);
            System.err.println(USAGE);
            System.exit(2);
        } catch (ConfigException e) {
            LOG.error("Invalid config, exiting abnormally", e);
            System.err.println("Invalid config, exiting abnormally");
            System.exit(2);
        } catch (Exception e) {
            LOG.error("Unexpected exception, exiting abnormally", e);
            System.exit(1);
        }
        LOG.info("Exiting normally");
        System.exit(0);
    }

    protected void initializeAndRun(String[] args)
        throws ConfigException, IOException
    {
        try {
            // 注册日志相关
            ManagedUtil.registerLog4jMBeans();
        } catch (JMException e) {
            LOG.warn("Unable to register log4j JMX control", e);
        }

        ServerConfig config = new ServerConfig();
        if (args.length == 1) {
            config.parse(args[0]);
        } else {
            config.parse(args);
        }

        runFromConfig(config);
    }

    /**
     * Run from a ServerConfig.
     * @param config ServerConfig to use.
     * @throws IOException
     */
    public void runFromConfig(ServerConfig config) throws IOException {
        LOG.info("Starting server");
        FileTxnSnapLog txnLog = null;
        try {
            // Note that this thread isn't going to be doing anything else,
            // so rather than spawning another thread, we will just call
            // run() in this thread.
            // create a file logger url from the command line args

            // 1) 里面创建ServerStats
            final ZooKeeperServer zkServer = new ZooKeeperServer();

            // Registers shutdown handler which will be used to know the
            // server error or shutdown state changes.
            final CountDownLatch shutdownLatch = new CountDownLatch(1);
            zkServer.registerServerShutdownHandler(new ZooKeeperServerShutdownHandler(shutdownLatch));

            // 2) FileTxnSnapLog类
            txnLog = new FileTxnSnapLog(new File(config.dataLogDir), new File(config.dataDir));
            zkServer.setTxnLogFactory(txnLog);
            // Zookeeper服务器之间或客户端与服务器之间维持心跳的时间间隔, 也就是每个tickTime时间就会发送一个心跳, tickTime以毫秒为单位
            zkServer.setTickTime(config.tickTime);
            // minSessionTimeout和maxSessionTimeout, 服务器可以通过这两个参数来限制客户端设置sessionTimeout的范围
            zkServer.setMinSessionTimeout(config.minSessionTimeout);
            zkServer.setMaxSessionTimeout(config.maxSessionTimeout);

            // 3) ServerCnxnFactory 类创建, 网络服务组件
            cnxnFactory = ServerCnxnFactory.createFactory();
            /**
             * maxClientCnxns: 单个客户端与单台服务器之间的连接数的限制, 是IP级别的, 默认是60, 如果设置为0, 那么表明不作任何限制
             */
            cnxnFactory.configure(config.getClientPortAddress(), config.getMaxClientCnxns());

            /**
             * 在通过ServerCnxnFactory类制定了具体的NIO框架类后.ZooKeeper首先会创建一个线程
             * Thread类作为ServerCnxnFactory类的启动主线程.之后ZooKeeper服务再初始化具体的NIO类.
             * 这里请你注意的是,虽然初始化完相关的 NIO 类 ,比如已经设置好了服务端的对外端口,客户端也
             * 能通过诸如 2181 端口等访问到服务端,但是此时 ZooKeeper 服务器还是无法处理客户端的请求操作.
             * 这是因为ZooKeeper启动后,还需要从本地的快照数据文件和事务日志文件中恢复数据.这之后才真正完成了ZooKeeper服务的启动.
             */
            cnxnFactory.startup(zkServer);

            /**
             * Watch status of ZooKeeper server. It will do a graceful shutdown
             * if the server is not running or hits an internal error.
             * 使用shutdownLatch实现优雅停机, 在shutdownLatch.countDown()调用之前会一直阻塞在这里
             * 调用shutdownLatch.countDown()之后就可以往下走了
             *
             * 什么情况下会调用shutdownLatch.countDown()?
             * 见ZooKeeperServer.setState方法, 当修改ZK Server状态时, 如果状态是ERROR或者SHUTDOWN会countDown
             */
            shutdownLatch.await();
            // 关闭ZK Server服务
            shutdown();

            cnxnFactory.join();
            if (zkServer.canShutdown()) {
                zkServer.shutdown(true);
            }
        } catch (InterruptedException e) {
            // warn, but generally this is ok
            LOG.warn("Server interrupted", e);
        } finally {
            if (txnLog != null) {
                txnLog.close();
            }
        }
    }

    /**
     * Shutdown the serving instance
     */
    protected void shutdown() {
        if (cnxnFactory != null) {
            cnxnFactory.shutdown();
        }
    }

    // VisibleForTesting
    ServerCnxnFactory getCnxnFactory() {
        return cnxnFactory;
    }
}

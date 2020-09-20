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
package org.apache.zookeeper.server.quorum;

import java.io.File;
import java.io.IOException;

import javax.management.JMException;
import javax.security.sasl.SaslException;

import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.jmx.ManagedUtil;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZKDatabase;
import org.apache.zookeeper.server.DatadirCleanupManager;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;

/**
 *
 * <h2>Configuration file</h2>
 *
 * When the main() method of this class is used to start the program, the first
 * argument is used as a path to the config file, which will be used to obtain
 * configuration information. This file is a Properties file, so keys and
 * values are separated by equals (=) and the key/value pairs are separated
 * by new lines. The following is a general summary of keys used in the
 * configuration file. For full details on this see the documentation in
 * docs/index.html
 * <ol>
 * <li>dataDir - The directory where the ZooKeeper data is stored.</li>
 * <li>dataLogDir - The directory where the ZooKeeper transaction log is stored.</li>
 * <li>clientPort - The port used to communicate with clients.</li>
 * <li>tickTime - The duration of a tick in milliseconds. This is the basic
 * unit of time in ZooKeeper.</li>
 * <li>initLimit - The maximum number of ticks that a follower will wait to
 * initially synchronize with a leader.</li>
 * <li>syncLimit - The maximum number of ticks that a follower will wait for a
 * message (including heartbeats) from the leader.</li>
 * <li>server.<i>id</i> - This is the host:port[:port] that the server with the
 * given id will use for the quorum protocol.</li>
 * </ol>
 * In addition to the config file. There is a file in the data directory called
 * "myid" that contains the server id as an ASCII decimal value.
 *
 */
@InterfaceAudience.Public
public class QuorumPeerMain {
    private static final Logger LOG = LoggerFactory.getLogger(QuorumPeerMain.class);

    private static final String USAGE = "Usage: QuorumPeerMain configfile";

    protected QuorumPeer quorumPeer;

    /**
     * To start the replicated server specify the configuration file name on
     * the command line.
     * @param args path to the configfile
     *
     * QuorumPeerMain是ZooKeeper服务的启动入口, 通常我们在控制台启动ZK服务时,
     * 输入zkServer.cm或zkServer.sh命令就是用来启动这个Java类的
     */
    public static void main(String[] args) {
        QuorumPeerMain main = new QuorumPeerMain();
        try {
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
        QuorumPeerConfig config = new QuorumPeerConfig();
        if (args.length == 1) {
            // 1) 解析配置文件
            config.parse(args[0]);
        }

        // Start and schedule the the purge task
        // 2) 创建日志文件清理器
        DatadirCleanupManager purgeMgr = new DatadirCleanupManager(config
                .getDataDir(), config.getDataLogDir(), config
                .getSnapRetainCount(), config.getPurgeInterval());
        purgeMgr.start();

        // 根据config.isDistributed判断, 如果配置参数中指定了相关的配置项, 并且指定了集群模式运行
        if (args.length == 1 && config.servers.size() > 0) {
            // 完成集群模式的初始化工作
            runFromConfig(config);
        } else {
            LOG.warn("Either no config or no quorum defined in config, running "
                    + " in standalone mode");
            // there is only server in the quorum -- run as standalone
            ZooKeeperServerMain.main(args);
        }
    }

    /**
     * 在 ZooKeeper 集群中将服务器分成 Leader 、Follow 、Observer 三种角色服务器,在集群运行期间这三种服务器所负责的工作各不相同
     *
     * 1) Leader角色服务器负责管理集群中其他的服务器,是集群中工作的分配和调度者
     *    Leader 角色服务器主要负责处理客户端发送的数据变更等事务性请求操作,并管理协调集群中的Follow角色服务器
     *
     * 2) Follow服务器的主要工作是选举出Leader服务器,在发生Leader服务器选举的时候,
     *    系统会从Follow服务器之间根据多数投票原则,选举出一个Follow服务器作为新的Leader服务器.
     *    Follow 服务器则主要处理客户端的获取数据等非事务性请求操作
     *
     * 3) Observer服务器则主要负责处理来自客户端的获取数据等请求,并不参与Leader服务器的选举操作,也不会作为候选者被选举为Leader服务器.
     *    Observer角色服务器的功能和Follow服务器相似,唯一的不同就是不参与Leader头节点服务器的选举工作
     */
    public void runFromConfig(QuorumPeerConfig config) throws IOException {
      try {
          // 日志相关
          ManagedUtil.registerLog4jMBeans();
      } catch (JMException e) {
          LOG.warn("Unable to register log4j JMX control", e);
      }
  
      LOG.info("Starting quorum peer");
      try {
          ServerCnxnFactory cnxnFactory = ServerCnxnFactory.createFactory();
          cnxnFactory.configure(config.getClientPortAddress(),
                                config.getMaxClientCnxns());

          /**
           * 在 ZooKeeper 服务的集群模式启动过程中,一个最主要的核心类是 QuorumPeer 类.
           * 我们可以将每个 QuorumPeer 类的实例看作集群中的一台服务器.在 ZooKeeper 集群模式的运行中,
           * 一个 QuorumPeer 类的实例通常具有 3 种状态,分别是参与 Leader 节点的选举、作为 Follow 节点
           * 同步 Leader 节点的数据,以及作为 Leader 节点管理集群中的 Follow 节点
           */
          quorumPeer = getQuorumPeer();

          quorumPeer.setQuorumPeers(config.getServers());

          /**
           * 在一个ZooKeeper服务的启动过程中,首先调用runFromConfig函数将服务运行过程中需要的核心工具类注册到QuorumPeer实例中去
           * 诸如FileTxnSnapLog 数据持久化类、ServerCnxnFactory类NIO工厂方法等.
           * 这之后还需要配置服务器地址列表、Leader 选举算法、会话超时时间等参数
           */
          quorumPeer.setTxnFactory(new FileTxnSnapLog(new File(config.getDataLogDir()), new File(config.getDataDir())));

          /**
           * 在 ZooKeeper 集群中,Leader 服务器负责管理集群中其他角色服务器,以及处理客户端的数据变更请求.
           * 因此,在整个 ZooKeeper 服务器中,Leader 服务器非常重要.所以在整个 ZooKeeper 集群启动过程中,
           * 首先要先选举出集群中的 Leader 服务器.
           *
           * 在 ZooKeeper 集群选举 Leader 节点的过程中,首先会根据服务器自身的服务器 ID(SID)、
           * 最新的 ZXID、和当前的服务器 epoch (currentEpoch)这三个参数来生成一个选举标准.
           * 之后, ZooKeeper 服务会根据 zoo.cfg 配置文件中的参数,选择参数文件中规定的 Leader 选举算法,
           * 进行 Leader 头节点的选举操作.而在 ZooKeeper 中提供了三种 Leader 选举算法,分别是
           * LeaderElection 、AuthFastLeaderElection、FastLeaderElection.在我们日常开发过程中,
           * 可以通过在 zoo.cfg 配置文件中使用 electionAlg 参数属性来制定具体要使用的算法类型
           */
          quorumPeer.setElectionType(config.getElectionAlg());

          // 本机启动的序号, 需要配置配置文件
          quorumPeer.setMyid(config.getServerId());
          quorumPeer.setTickTime(config.getTickTime());
          quorumPeer.setInitLimit(config.getInitLimit());
          quorumPeer.setSyncLimit(config.getSyncLimit());
          quorumPeer.setQuorumListenOnAllIPs(config.getQuorumListenOnAllIPs());
          quorumPeer.setCnxnFactory(cnxnFactory);

          // 事务提交过程中验证算法, 默认大多数同意就提交
          quorumPeer.setQuorumVerifier(config.getQuorumVerifier());
          quorumPeer.setClientPortAddress(config.getClientPortAddress());
          quorumPeer.setMinSessionTimeout(config.getMinSessionTimeout());
          quorumPeer.setMaxSessionTimeout(config.getMaxSessionTimeout());
          quorumPeer.setZKDatabase(new ZKDatabase(quorumPeer.getTxnFactory()));
          quorumPeer.setLearnerType(config.getPeerType());
          quorumPeer.setSyncEnabled(config.getSyncEnabled());

          // sets quorum sasl authentication configurations
          quorumPeer.setQuorumSaslEnabled(config.quorumEnableSasl);
          if(quorumPeer.isQuorumSaslAuthEnabled()){
              quorumPeer.setQuorumServerSaslRequired(config.quorumServerRequireSasl);
              quorumPeer.setQuorumLearnerSaslRequired(config.quorumLearnerRequireSasl);
              quorumPeer.setQuorumServicePrincipal(config.quorumServicePrincipal);
              quorumPeer.setQuorumServerLoginContext(config.quorumServerLoginContext);
              quorumPeer.setQuorumLearnerLoginContext(config.quorumLearnerLoginContext);
          }

          quorumPeer.setQuorumCnxnThreadsSize(config.quorumCnxnThreadsSize);
          quorumPeer.initialize();

          quorumPeer.start();
          quorumPeer.join();
      } catch (InterruptedException e) {
          // warn, but generally this is ok
          LOG.warn("Quorum Peer interrupted", e);
      }
    }

    // @VisibleForTesting
    protected QuorumPeer getQuorumPeer() throws SaslException {
        return new QuorumPeer();
    }
}

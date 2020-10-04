/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.server.quorum;

import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.RequestProcessor;
import org.apache.zookeeper.server.SyncRequestProcessor;
import org.apache.zookeeper.server.quorum.Leader.XidRolloverException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This RequestProcessor simply forwards requests to an AckRequestProcessor and SyncRequestProcessor.
 *
 * PrepRequestProcessor预处理器执行完工作后,就轮到ProposalRequestProcessor事物处理器上场了,
 * ProposalRequestProcessor是继PrepRequestProcessor后,责任链模式上的第二个处理器.其主要作用
 * 就是对事务性的请求操作进行处理,而从ProposalRequestProcessor处理器的名字中就能大概猜出,其具体
 * 的工作就是提议.所谓的提议是说,当处理一个事务性请求的时候,ZooKeeper首先会在服务端发起一次投票流程,
 * 该投票的主要作用就是通知ZooKeeper服务端的各个机器进行事务性的操作了,避免因为某个机器出现问题而造成事务
 * 不一致等问题.在ProposalRequestProcessor处理器阶段,其内部又分成了三个子流程,
 * 分别是Sync 流程、Proposal 流程、Commit 流程
 *
 * Sync流程
 * 首先我们看一下Sync流程,该流程的底层实现类是SyncRequestProcess类.
 * SyncRequestProcessor类的作用就是在处理事务性请求时,ZooKeeper服务中的每台机器都将该条请求的操作日志记录下来,
 * 完成这个操作后,每一台机器都会向ZooKeeper服务中的Leader机器发送事物日志记录完成的通知.
 *
 * Proposal流程
 * 在处理事务性请求的过程中,ZooKeeper需要取得在集群中过半数机器的投票,只有在这种情况下才能真正地将数据改变.
 * 而Proposal流程的主要工作就是投票和统计投票结果.投票的方式大体上遵循多数原则
 *
 * Commit 流程
 * 请你注意,在完成Proposal流程后,ZooKeeper服务器上的数据不会进行任何改变,成功通过Proposal流程只是说明
 * ZooKeeper服务可以执行事务性的请求操作了,而要真正执行具体数据变更,需要在Commit流程中实现,
 * 这种实现方式很像是MySQL等数据库的操作方式.在Commit流程中,它的主要作用就是完成请求的执行.
 * 其底层实现是通过CommitProcessor实现的.CommitProcessor类的内部有一个LinkedList类型的queuedRequests队列,
 * queuedRequests队列的作用是,当CommitProcessor收到请求后,并不会立刻对该条请求进行处理,
 * 而是将其放在queuedRequests 队列中.
 */
public class ProposalRequestProcessor implements RequestProcessor {
    private static final Logger LOG =
            LoggerFactory.getLogger(ProposalRequestProcessor.class);

    LeaderZooKeeperServer zks;

    RequestProcessor nextProcessor;

    SyncRequestProcessor syncProcessor;

    /**
     * ProposalRequestProcessor下一步对应两个Processor
     */
    public ProposalRequestProcessor(LeaderZooKeeperServer zks, RequestProcessor nextProcessor) {
        this.zks = zks;
        // 一个是CommitProcessor, 也就是这里通过构造函数传进来的nextProcessor
        this.nextProcessor = nextProcessor;

        AckRequestProcessor ackProcessor = new AckRequestProcessor(zks.getLeader());
        // 还有一个就是SyncRequestProcessor, SyncRequestProcessor接下来的就是AckRequestProcessor
        syncProcessor = new SyncRequestProcessor(zks, ackProcessor);
    }

    /**
     * initialize this processor
     */
    public void initialize() {
        syncProcessor.start();
    }

    public void processRequest(Request request) throws RequestProcessorException {
        // LOG.warn("Ack>>> cxid = " + request.cxid + " type = " +
        // request.type + " id = " + request.sessionId);
        // request.addRQRec(">prop");


        /* In the following IF-THEN-ELSE block, we process syncs on the leader.
         * If the sync is coming from a follower, then the follower
         * handler adds it to syncHandler. Otherwise, if it is a client of
         * the leader that issued the sync command, then syncHandler won't
         * contain the handler. In this case, we add it to syncHandler, and
         * call processRequest on the next processor.
         */

        if (request instanceof LearnerSyncRequest) {
            zks.getLeader().processSync((LearnerSyncRequest) request);
        } else {
            // nextProcessor是CommitProcessor
            nextProcessor.processRequest(request);
            // 判断是不是事务消息, 只有事务消息hdr才有值
            if (request.hdr != null) {
                try {
                    /**
                     * We need to sync and get consensus on any transactions
                     * Proposal流程. 在ZooKeeper实现中, 每个事务请求都需要集群中的过半机器投票认可才能真正被应用到ZooKeeper的
                     * 内存数据库中去, 这个过程被称为Proposal过程
                     */
                    zks.getLeader().propose(request);
                } catch (XidRolloverException e) {
                    throw new RequestProcessorException(e.getMessage(), e);
                }
                /**
                 * sync流程. 核心就是使用SyncRequestProcessor处理器记录事务日志的过程. ProposalRequestProcessor处理器在接收到
                 * 上一个上级处理器流转过来的请求时, 首先会判断该请求是否是事务请求, 针对每个事务请求, 都会通过事务日志的形式的将其
                 * 记录下来. 完成事务日志之后, 每个Follower服务器都会向Leader服务器发送ACK消息, 表明自身完成了事务日志的记录, 以便
                 * Leader服务器统计每个事务请求的投票情况
                 */
                syncProcessor.processRequest(request);
            }
        }
    }

    public void shutdown() {
        LOG.info("Shutting down");
        nextProcessor.shutdown();
        syncProcessor.shutdown();
    }

}

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
 * SyncRequestProcesor类的作用就是在处理事务性请求时,ZooKeeper服务中的每台机器都将该条请求的操作日志记录下来,
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

    public ProposalRequestProcessor(LeaderZooKeeperServer zks,
                                    RequestProcessor nextProcessor) {
        this.zks = zks;
        this.nextProcessor = nextProcessor;
        AckRequestProcessor ackProcessor = new AckRequestProcessor(zks.getLeader());
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
            nextProcessor.processRequest(request);
            // 判断是不是事务消息, 只有事务消息hdr才有值
            if (request.hdr != null) {
                // We need to sync and get consensus on any transactions
                try {
                    zks.getLeader().propose(request);
                } catch (XidRolloverException e) {
                    throw new RequestProcessorException(e.getMessage(), e);
                }
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

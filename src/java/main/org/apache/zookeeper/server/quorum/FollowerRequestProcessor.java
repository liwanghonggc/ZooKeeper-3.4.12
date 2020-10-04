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

import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.server.RequestProcessor;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.ZooKeeperCriticalThread;
import org.apache.zookeeper.server.ZooTrace;

/**
 * This RequestProcessor forwards any requests that modify the state of the system to the Leader.
 * 它是Follower服务器的第一个请求处理器, 主要是识别出当前请求是否是事务请求. 如果是事务请求, 那么Follower就会
 * 将该事务请求转发给leader服务器. Leader服务器在接收到这个事务请求之后, 就会将其提交到请求处理链, 按照正常
 * 事务请求进行处理
 */
public class FollowerRequestProcessor extends ZooKeeperCriticalThread implements
        RequestProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(FollowerRequestProcessor.class);

    FollowerZooKeeperServer zks;

    RequestProcessor nextProcessor;

    LinkedBlockingQueue<Request> queuedRequests = new LinkedBlockingQueue<Request>();

    boolean finished = false;

    public FollowerRequestProcessor(FollowerZooKeeperServer zks,
            RequestProcessor nextProcessor) {
        super("FollowerRequestProcessor:" + zks.getServerId(), zks.getZooKeeperServerListener());
        this.zks = zks;
        this.nextProcessor = nextProcessor;
    }

    @Override
    public void run() {
        try {
            while (!finished) {
                Request request = queuedRequests.take();
                if (LOG.isTraceEnabled()) {
                    ZooTrace.logRequest(LOG, ZooTrace.CLIENT_REQUEST_TRACE_MASK,
                            'F', request, "");
                }
                if (request == Request.requestOfDeath) {
                    break;
                }
                // We want to queue the request to be processed before we submit
                // the request to the leader so that we are ready to receive
                // the response
                nextProcessor.processRequest(request);

                /**
                 * We now ship the request to the leader. As with all other quorum operations,
                 * sync also follows this code path, but different from others, we need to keep track
                 * of the sync operations this follower has pending, so we add it to pendingSyncs.
                 *
                 * 所有非Leader服务器如果接收到了来自客户端的事务请求, 那么必须将其转发给Leader服务器来处理
                 * 如果是事务请求, 该请求将被以REQUEST消息的形式转给给Leader服务器, Leader服务器在接收到这个消息
                 * 之后, 会解析出客户端的原始请求, 然后提交到自己的请求处理链中开始进行事务请求的处理
                 */
                switch (request.type) {
                case OpCode.sync:
                    zks.pendingSyncs.add(request);
                    zks.getFollower().request(request);
                    break;
                case OpCode.create:
                case OpCode.delete:
                case OpCode.setData:
                case OpCode.setACL:
                case OpCode.createSession:
                case OpCode.closeSession:
                case OpCode.multi:
                    zks.getFollower().request(request);
                    break;
                }
            }
        } catch (Exception e) {
            handleException(this.getName(), e);
        }
        LOG.info("FollowerRequestProcessor exited loop!");
    }

    public void processRequest(Request request) {
        if (!finished) {
            queuedRequests.add(request);
        }
    }

    public void shutdown() {
        LOG.info("Shutting down");
        finished = true;
        queuedRequests.clear();
        queuedRequests.add(Request.requestOfDeath);
        nextProcessor.shutdown();
    }

}

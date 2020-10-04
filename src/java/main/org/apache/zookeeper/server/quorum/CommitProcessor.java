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

import java.util.ArrayList;
import java.util.LinkedList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.RequestProcessor;
import org.apache.zookeeper.server.ZooKeeperCriticalThread;
import org.apache.zookeeper.server.ZooKeeperServerListener;

/**
 * This RequestProcessor matches the incoming committed requests with the
 * locally submitted requests. The trick is that locally submitted requests that
 * change the state of the system will come back as incoming committed requests,
 * so we need to match them up.
 *
 * CommitProcessor类的内部有一个LinkedList类型的queuedRequests队列,queuedRequests队列的作用是,
 * 当CommitProcessor收到请求后,并不会立刻对该条请求进行处理,而是将其放在queuedRequests队列中.
 *
 * 它是事务提交处理器. 对于非事务请求, 该处理器会直接将其交付给下一级处理器进行处理. 而对于事务请求,
 * CommitProcessor处理器会等待集群内针对Proposal的投票直到该Proposal可被提交. 利用CommitProcessor,
 * 每个服务器都可以很好地控制对事务请求的顺序处理
 */
public class CommitProcessor extends ZooKeeperCriticalThread implements RequestProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(CommitProcessor.class);

    /**
     * Requests that we are holding until the commit comes in.
     */
    LinkedList<Request> queuedRequests = new LinkedList<Request>();

    /**
     * Requests that have been committed.
     */
    LinkedList<Request> committedRequests = new LinkedList<Request>();

    RequestProcessor nextProcessor;
    ArrayList<Request> toProcess = new ArrayList<Request>();

    /**
     * This flag indicates whether we need to wait for a response to come back from the
     * leader or we just let the sync operation flow through like a read. The flag will
     * be false if the CommitProcessor is in a Leader pipeline.
     */
    boolean matchSyncs;

    public CommitProcessor(RequestProcessor nextProcessor, String id,
            boolean matchSyncs, ZooKeeperServerListener listener) {
        super("CommitProcessor:" + id, listener);
        this.nextProcessor = nextProcessor;
        this.matchSyncs = matchSyncs;
    }

    volatile boolean finished = false;

    @Override
    public void run() {
        try {
            Request nextPending = null;            
            while (!finished) {
                int len = toProcess.size();
                for (int i = 0; i < len; i++) {
                    // FinalRequestProcessor
                    nextProcessor.processRequest(toProcess.get(i));
                }
                toProcess.clear();

                synchronized (this) {
                    /**
                     * 如果从queuedRequests队列里面取出来的请求是一个事务请求, 那么就需要进行集群中各服务器之间的投票处理.
                     * 同时需要将nextPending标记为当前请求. 作用是, 一方面是为了确保事务请求的顺序型. 另一方面是为了便于
                     * CommitProcessor处理器检测当前集群中是否有正在进行事务请求的投票
                     */
                    if ((queuedRequests.size() == 0 || nextPending != null) && committedRequests.size() == 0) {
                        wait();
                        continue;
                    }

                    /**
                     * First check and see if the commit came in for the pending request
                     * 一旦发现committedRequests队列里面已经有可以提交的请求了, 那么Commit流程就会开始提交请求.
                     * 在提交之前, 为了保证事务请求的顺序型, Commit流程还会对比之前标记的nextPending和committedRequests
                     * 队列中的第一个请求是否一致. 如果检查通过, 那么Commit流程就会就会将请求放入toProcess队列中, 然后交付给下一个
                     * 请求处理器:FinalRequestProcessor
                     */
                    if ((queuedRequests.size() == 0 || nextPending != null) && committedRequests.size() > 0) {
                        Request r = committedRequests.remove();
                        /*
                         * We match with nextPending so that we can move to the
                         * next request when it is committed. We also want to
                         * use nextPending because it has the cnxn member set
                         * properly.
                         */
                        if (nextPending != null
                                && nextPending.sessionId == r.sessionId
                                && nextPending.cxid == r.cxid) {
                            // we want to send our version of the request.
                            // the pointer to the connection in the request
                            nextPending.hdr = r.hdr;
                            nextPending.txn = r.txn;
                            nextPending.zxid = r.zxid;
                            toProcess.add(nextPending);
                            nextPending = null;
                        } else {
                            // this request came from someone else so just send the commit packet
                            toProcess.add(r);
                        }
                    }
                }

                // We haven't matched the pending requests, so go back to waiting
                if (nextPending != null) {
                    continue;
                }

                synchronized (this) {
                    // Process the next requests in the queuedRequests
                    while (nextPending == null && queuedRequests.size() > 0) {
                        Request request = queuedRequests.remove();
                        switch (request.type) {
                        case OpCode.create:
                        case OpCode.delete:
                        case OpCode.setData:
                        case OpCode.multi:
                        case OpCode.setACL:
                        case OpCode.createSession:
                        case OpCode.closeSession:
                            nextPending = request;
                            break;
                        case OpCode.sync:
                            if (matchSyncs) {
                                nextPending = request;
                            } else {
                                toProcess.add(request);
                            }
                            break;
                        default:
                            toProcess.add(request);
                        }
                    }
                }
            }
        } catch (InterruptedException e) {
            LOG.warn("Interrupted exception while waiting", e);
        } catch (Throwable e) {
            LOG.error("Unexpected exception causing CommitProcessor to exit", e);
        }
        LOG.info("CommitProcessor exited loop!");
    }

    synchronized public void commit(Request request) {
        if (!finished) {
            if (request == null) {
                LOG.warn("Committed a null!",
                         new Exception("committing a null! "));
                return;
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("Committing request:: " + request);
            }
            /**
             * 如果一个提议已经获得了过半机器的投票认可, 那么将会进入请求提交阶段
             * ZooKeeper会将该请求放入commitedRequests队列中, 同时唤醒Commit流程
             */
            committedRequests.add(request);
            notifyAll();
        }
    }

    synchronized public void processRequest(Request request) {
        // request.addRQRec(">commit");
        if (LOG.isDebugEnabled()) {
            LOG.debug("Processing request:: " + request);
        }

        // CommitProcessor收到请求后不会立即处理, 而是将其放入queuedRequest队列中
        if (!finished) {
            queuedRequests.add(request);
            notifyAll();
        }
    }

    public void shutdown() {
        LOG.info("Shutting down");
        synchronized (this) {
            finished = true;
            queuedRequests.clear();
            notifyAll();
        }
        if (nextProcessor != null) {
            nextProcessor.shutdown();
        }
    }

}

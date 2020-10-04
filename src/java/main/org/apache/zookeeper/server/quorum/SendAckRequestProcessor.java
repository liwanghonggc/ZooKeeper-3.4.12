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

import java.io.Flushable;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.RequestProcessor;

/**
 * Leader服务器上有一个AckRequestProcessor的请求处理器, 负责在SyncRequestProcessor处理器完成事务日志之后, 向Proposal的投票器进行反馈.
 * 而在Follow服务器上, SendAckRequestProcessor处理器同样承担了事务日志记录反馈的角色, 在完成事务日志记录之后, 会向Leader服务器发送ACK
 * 消息以表明自身完成了事务日志的记录工作. 两者的唯一区别在于, AckRequestProcessor处理器和Leader服务器在同一个服务器上面, 因此它的ACK
 * 反馈仅仅是一个本地操作, 而SendAckRequestProcessor处理器由于在Follow服务器上, 因此需要通过以ACK消息的形式来向Leader服务器进行反馈
 */
public class SendAckRequestProcessor implements RequestProcessor, Flushable {
    private static final Logger LOG = LoggerFactory.getLogger(SendAckRequestProcessor.class);
    
    Learner learner;

    SendAckRequestProcessor(Learner peer) {
        this.learner = peer;
    }

    public void processRequest(Request si) {
        if(si.type != OpCode.sync){
            QuorumPacket qp = new QuorumPacket(Leader.ACK, si.hdr.getZxid(), null,
                null);
            try {
                learner.writePacket(qp, false);
            } catch (IOException e) {
                LOG.warn("Closing connection to leader, exception during packet send", e);
                try {
                    if (!learner.sock.isClosed()) {
                        learner.sock.close();
                    }
                } catch (IOException e1) {
                    // Nothing to do, we are shutting things down, so an exception here is irrelevant
                    LOG.debug("Ignoring error closing the connection", e1);
                }
            }
        }
    }
    
    public void flush() throws IOException {
        try {
            learner.writePacket(null, true);
        } catch(IOException e) {
            LOG.warn("Closing connection to leader, exception during packet send", e);
            try {
                if (!learner.sock.isClosed()) {
                    learner.sock.close();
                }
            } catch (IOException e1) {
                    // Nothing to do, we are shutting things down, so an exception here is irrelevant
                    LOG.debug("Ignoring error closing the connection", e1);
            }
        }
    }

    public void shutdown() {
        // Nothing needed
    }

}

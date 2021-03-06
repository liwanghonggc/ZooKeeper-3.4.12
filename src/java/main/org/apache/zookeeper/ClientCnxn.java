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

package org.apache.zookeeper;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.Thread.UncaughtExceptionHandler;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.LinkedBlockingQueue;

import javax.security.auth.login.LoginException;
import javax.security.sasl.SaslException;

import org.apache.jute.BinaryInputArchive;
import org.apache.jute.BinaryOutputArchive;
import org.apache.jute.Record;
import org.apache.zookeeper.AsyncCallback.ACLCallback;
import org.apache.zookeeper.AsyncCallback.Children2Callback;
import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.MultiCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.OpResult.ErrorResult;
import org.apache.zookeeper.Watcher.Event;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.ZooKeeper.WatchRegistration;
import org.apache.zookeeper.client.HostProvider;
import org.apache.zookeeper.client.ZooKeeperSaslClient;
import org.apache.zookeeper.common.Time;
import org.apache.zookeeper.proto.AuthPacket;
import org.apache.zookeeper.proto.ConnectRequest;
import org.apache.zookeeper.proto.CreateResponse;
import org.apache.zookeeper.proto.ExistsResponse;
import org.apache.zookeeper.proto.GetACLResponse;
import org.apache.zookeeper.proto.GetChildren2Response;
import org.apache.zookeeper.proto.GetChildrenResponse;
import org.apache.zookeeper.proto.GetDataResponse;
import org.apache.zookeeper.proto.GetSASLRequest;
import org.apache.zookeeper.proto.ReplyHeader;
import org.apache.zookeeper.proto.RequestHeader;
import org.apache.zookeeper.proto.SetACLResponse;
import org.apache.zookeeper.proto.SetDataResponse;
import org.apache.zookeeper.proto.SetWatches;
import org.apache.zookeeper.proto.WatcherEvent;
import org.apache.zookeeper.server.ByteBufferInputStream;
import org.apache.zookeeper.server.ZooKeeperThread;
import org.apache.zookeeper.server.ZooTrace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class manages the socket i/o for the client. ClientCnxn maintains a list
 * of available servers to connect to and "transparently" switches servers it is
 * connected to as needed.
 *
 */
public class ClientCnxn {
    private static final Logger LOG = LoggerFactory.getLogger(ClientCnxn.class);

    private static final String ZK_SASL_CLIENT_USERNAME =
            "zookeeper.sasl.client.username";

    /* ZOOKEEPER-706: If a session has a large number of watches set then
     * attempting to re-establish those watches after a connection loss may
     * fail due to the SetWatches request exceeding the server's configured
     * jute.maxBuffer value. To avoid this we instead split the watch
     * re-establishement across multiple SetWatches calls. This constant
     * controls the size of each call. It is set to 128kB to be conservative
     * with respect to the server's 1MB default for jute.maxBuffer.
     */
    private static final int SET_WATCHES_MAX_LENGTH = 128 * 1024;

    /** This controls whether automatic watch resetting is enabled.
     * Clients automatically reset watches during session reconnect, this
     * option allows the client to turn off this behavior by setting
     * the environment variable "zookeeper.disableAutoWatchReset" to "true" */
    private static boolean disableAutoWatchReset;

    static {
        // this var should not be public, but otw there is no easy way
        // to test
        disableAutoWatchReset = Boolean.getBoolean("zookeeper.disableAutoWatchReset");
        if (LOG.isDebugEnabled()) {
            LOG.debug("zookeeper.disableAutoWatchReset is "
                    + disableAutoWatchReset);
        }
    }

    static class AuthData {
        AuthData(String scheme, byte data[]) {
            this.scheme = scheme;
            this.data = data;
        }

        String scheme;

        byte data[];
    }

    private final CopyOnWriteArraySet<AuthData> authInfo = new CopyOnWriteArraySet<AuthData>();

    /**
     * These are the packets that have been sent and are waiting for a response.
     */
    private final LinkedList<Packet> pendingQueue = new LinkedList<Packet>();

    /**
     * These are the packets that need to be sent.
     */
    private final LinkedList<Packet> outgoingQueue = new LinkedList<Packet>();

    private int connectTimeout;

    /**
     * The timeout in ms the client negotiated with the server. This is the
     * "real" timeout, not the timeout request by the client (which may have
     * been increased/decreased by the server which applies bounds to this
     * value.
     */
    private volatile int negotiatedSessionTimeout;

    private int readTimeout;

    private final int sessionTimeout;

    private final ZooKeeper zooKeeper;

    private final ClientWatchManager watcher;

    private long sessionId;

    private byte sessionPasswd[] = new byte[16];

    /**
     * If true, the connection is allowed to go to r-o mode. This field's value
     * is sent, besides other data, during session creation handshake. If the
     * server on the other side of the wire is partitioned it'll accept
     * read-only clients only.
     */
    private boolean readOnly;

    final String chrootPath;

    final SendThread sendThread;

    final EventThread eventThread;

    /**
     * Set to true when close is called. Latches the connection such that we
     * don't attempt to re-connect to the server if in the middle of closing the
     * connection (client sends session disconnect to server as part of close
     * operation)
     */
    private volatile boolean closing = false;

    /**
     * A set of ZooKeeper hosts this client could connect to.
     */
    private final HostProvider hostProvider;

    /**
     * Is set to true when a connection to a r/w server is established for the
     * first time; never changed afterwards.
     * <p>
     * Is used to handle situations when client without sessionId connects to a
     * read-only server. Such client receives "fake" sessionId from read-only
     * server, but this sessionId is invalid for other servers. So when such
     * client finds a r/w server, it sends 0 instead of fake sessionId during
     * connection handshake and establishes new, valid session.
     * <p>
     * If this field is false (which implies we haven't seen r/w server before)
     * then non-zero sessionId is fake, otherwise it is valid.
     */
    volatile boolean seenRwServerBefore = false;


    public ZooKeeperSaslClient zooKeeperSaslClient;

    public long getSessionId() {
        return sessionId;
    }

    public byte[] getSessionPasswd() {
        return sessionPasswd;
    }

    public int getSessionTimeout() {
        return negotiatedSessionTimeout;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        SocketAddress local = sendThread.getClientCnxnSocket().getLocalSocketAddress();
        SocketAddress remote = sendThread.getClientCnxnSocket().getRemoteSocketAddress();
        sb
                .append("sessionid:0x").append(Long.toHexString(getSessionId()))
                .append(" local:").append(local)
                .append(" remoteserver:").append(remote)
                .append(" lastZxid:").append(lastZxid)
                .append(" xid:").append(xid)
                .append(" sent:").append(sendThread.getClientCnxnSocket().getSentCount())
                .append(" recv:").append(sendThread.getClientCnxnSocket().getRecvCount())
                .append(" queuedpkts:").append(outgoingQueue.size())
                .append(" pendingresp:").append(pendingQueue.size())
                .append(" queuedevents:").append(eventThread.waitingEvents.size());

        return sb.toString();
    }

    /**
     * This class allows us to pass the headers and the relevant records around.
     */
    static class Packet {
        /**
         * 请求头信息
         */
        RequestHeader requestHeader;

        /**
         * 响应头信息
         */
        ReplyHeader replyHeader;

        /**
         * 请求信息体
         */
        Record request;

        /**
         * 响应信息体
         */
        Record response;

        /**
         * 请求信息都是放在ByteBuffer上面
         */
        ByteBuffer bb;

        /** Client's view of the path (may differ due to chroot) **/
        String clientPath;
        /** Servers's view of the path (may differ due to chroot) **/
        String serverPath;

        boolean finished;

        AsyncCallback cb;

        Object ctx;

        /**
         * Watch监控信息
         */
        WatchRegistration watchRegistration;

        public boolean readOnly;

        /** Convenience ctor */
        Packet(RequestHeader requestHeader, ReplyHeader replyHeader,
               Record request, Record response,
               WatchRegistration watchRegistration) {
            this(requestHeader, replyHeader, request, response,
                    watchRegistration, false);
        }

        Packet(RequestHeader requestHeader, ReplyHeader replyHeader,
               Record request, Record response,
               WatchRegistration watchRegistration, boolean readOnly) {

            this.requestHeader = requestHeader;
            this.replyHeader = replyHeader;
            this.request = request;
            this.response = response;
            this.readOnly = readOnly;
            this.watchRegistration = watchRegistration;
        }

        /**
         * 对Packet对象进行序列化, 以便网络传输
         *
         * 在createBB进行序列化的时候,并不是将Packet类中的所有属性字段进行序列化.
         * 而是只对请求头信息(requestHeader)、请求体信息(request)、只读(readOnly)这三个属性字段进行序列化.
         * 而其余的属性字段则只是存储在客户端,用于之后的相关操作
         */
        public void createBB() {
            try {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                BinaryOutputArchive boa = BinaryOutputArchive.getArchive(baos);
                boa.writeInt(-1, "len"); // We'll fill this in later
                if (requestHeader != null) {
                    requestHeader.serialize(boa, "header");
                }
                if (request instanceof ConnectRequest) {
                    request.serialize(boa, "connect");
                    // append "am-I-allowed-to-be-readonly" flag
                    boa.writeBool(readOnly, "readOnly");
                } else if (request != null) {
                    request.serialize(boa, "request");
                }
                baos.close();
                this.bb = ByteBuffer.wrap(baos.toByteArray());
                this.bb.putInt(this.bb.capacity() - 4);
                this.bb.rewind();
            } catch (IOException e) {
                System.out.println("Exception");
                LOG.warn("Ignoring unexpected exception", e);
            }
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();

            sb.append(" clientPath:" + clientPath);
            sb.append(" serverPath:" + serverPath);
            sb.append(" finished:" + finished);

            sb.append(" header:: " + requestHeader);
            sb.append(" replyHeader:: " + replyHeader);
            sb.append(" request:: " + request);
            sb.append(" response:: " + response);

            // jute toString is horrible, remove unnecessary newlines
            return sb.toString().replaceAll("\r*\n+", " ");
        }
    }

    /**
     * Creates a connection object. The actual network connect doesn't get
     * established until needed. The start() instance method must be called
     * subsequent to construction.
     *
     * @param chrootPath - the chroot of this client. Should be removed from this Class in ZOOKEEPER-838
     * @param hostProvider
     *                the list of ZooKeeper servers to connect to
     * @param sessionTimeout
     *                the timeout for connections.
     * @param zooKeeper
     *                the zookeeper object that this connection is related to.
     * @param watcher watcher for this connection
     * @param clientCnxnSocket
     *                the socket implementation used (e.g. NIO/Netty)
     * @param canBeReadOnly
     *                whether the connection is allowed to go to read-only
     *                mode in case of partitioning
     * @throws IOException
     */
    public ClientCnxn(String chrootPath, HostProvider hostProvider, int sessionTimeout, ZooKeeper zooKeeper,
                      ClientWatchManager watcher, ClientCnxnSocket clientCnxnSocket, boolean canBeReadOnly)
            throws IOException {
        this(chrootPath, hostProvider, sessionTimeout, zooKeeper, watcher, clientCnxnSocket, 0, new byte[16], canBeReadOnly);
    }

    /**
     * Creates a connection object. The actual network connect doesn't get
     * established until needed. The start() instance method must be called
     * subsequent to construction.
     *
     * @param chrootPath - the chroot of this client. Should be removed from this Class in ZOOKEEPER-838
     * @param hostProvider
     *                the list of ZooKeeper servers to connect to
     * @param sessionTimeout
     *                the timeout for connections.
     * @param zooKeeper
     *                the zookeeper object that this connection is related to.
     * @param watcher watcher for this connection
     * @param clientCnxnSocket
     *                the socket implementation used (e.g. NIO/Netty)
     * @param sessionId session id if re-establishing session
     * @param sessionPasswd session passwd if re-establishing session
     * @param canBeReadOnly
     *                whether the connection is allowed to go to read-only
     *                mode in case of partitioning
     * @throws IOException
     */
    public ClientCnxn(String chrootPath, HostProvider hostProvider, int sessionTimeout, ZooKeeper zooKeeper,
                      ClientWatchManager watcher, ClientCnxnSocket clientCnxnSocket,
                      long sessionId, byte[] sessionPasswd, boolean canBeReadOnly) {
        this.zooKeeper = zooKeeper;
        this.watcher = watcher;
        this.sessionId = sessionId;
        this.sessionPasswd = sessionPasswd;
        this.sessionTimeout = sessionTimeout;
        this.hostProvider = hostProvider;
        this.chrootPath = chrootPath;

        connectTimeout = sessionTimeout / hostProvider.size();
        readTimeout = sessionTimeout * 2 / 3;
        readOnly = canBeReadOnly;

        sendThread = new SendThread(clientCnxnSocket);
        eventThread = new EventThread();

    }

    /**
     * tests use this to check on reset of watches
     * @return if the auto reset of watches are disabled
     */
    public static boolean getDisableAutoResetWatch() {
        return disableAutoWatchReset;
    }

    /**
     * tests use this to set the auto reset
     * @param b the value to set disable watches to
     */
    public static void setDisableAutoResetWatch(boolean b) {
        disableAutoWatchReset = b;
    }

    public void start() {
        sendThread.start();
        eventThread.start();
    }

    private Object eventOfDeath = new Object();

    private static class WatcherSetEventPair {
        private final Set<Watcher> watchers;
        private final WatchedEvent event;

        public WatcherSetEventPair(Set<Watcher> watchers, WatchedEvent event) {
            this.watchers = watchers;
            this.event = event;
        }
    }

    /**
     * Guard against creating "-EventThread-EventThread-EventThread-..." thread
     * names when ZooKeeper object is being created from within a watcher.
     * See ZOOKEEPER-795 for details.
     */
    private static String makeThreadName(String suffix) {
        String name = Thread.currentThread().getName().replaceAll("-EventThread", "");
        return name + suffix;
    }

    /**
     * SendThread 类的主要工作可以简单地理解为负责客户端向服务端发送请求等操作.
     * 而像我们之前学到的Watch监控机制,在事件触发后ZooKeeper服务端会发送通知给相关的客户端,那么在这个过程中,客户端是如何接收服务端的请求的呢?
     *
     * ZooKeeper是通过EventThread类来实现的,EventThread类也是一个线程类,主要负责客户端的事件处理,比如在客户端接收Watch通知时,
     * 触发客户端的相关方法.在EventThread类中,通过将要触发的事件对象存放在waitingEvents队列中,之后在接收到相应的事件通知时,
     * 会从该队列中取出对应的事件信息,之后调用process函数进行处理
     */
    class EventThread extends ZooKeeperThread {

        /**
         * 将要触发的事件对象存放在waitingEvents队列中
         */
        private final LinkedBlockingQueue<Object> waitingEvents = new LinkedBlockingQueue<Object>();

        /** This is really the queued session state until the event
         * thread actually processes the event and hands it to the watcher.
         * But for all intents and purposes this is the state.
         */
        private volatile KeeperState sessionState = KeeperState.Disconnected;

        private volatile boolean wasKilled = false;
        private volatile boolean isRunning = false;

        EventThread() {
            super(makeThreadName("-EventThread"));
            setDaemon(true);
        }

        public void queueEvent(WatchedEvent event) {
            if (event.getType() == EventType.None && sessionState == event.getState()) {
                return;
            }
            sessionState = event.getState();

            // materialize the watchers based on the event
            WatcherSetEventPair pair = new WatcherSetEventPair(
                    watcher.materialize(event.getState(), event.getType(),
                            event.getPath()),
                    event);
            // queue the pair (watch set & event) for later processing
            waitingEvents.add(pair);
        }

        public void queuePacket(Packet packet) {
            if (wasKilled) {
                synchronized (waitingEvents) {
                    if (isRunning) waitingEvents.add(packet);
                    else processEvent(packet);
                }
            } else {
                waitingEvents.add(packet);
            }
        }

        public void queueEventOfDeath() {
            waitingEvents.add(eventOfDeath);
        }

        @Override
        public void run() {
            try {
                isRunning = true;
                while (true) {
                    // 从里面获取一个event进行处理
                    Object event = waitingEvents.take();

                    // 在sendThread.onConnected方法里, 如果会话超期了就会往waitingEvents里面加入eventOfDeath事件
                    if (event == eventOfDeath) {
                        wasKilled = true;
                    } else {
                        // 处理
                        processEvent(event);
                    }
                    if (wasKilled)
                        synchronized (waitingEvents) {
                            if (waitingEvents.isEmpty()) {
                                isRunning = false;
                                break;
                            }
                        }
                }
            } catch (InterruptedException e) {
                LOG.error("Event thread exiting due to interruption", e);
            }

            LOG.info("EventThread shut down for session: 0x{}",
                    Long.toHexString(getSessionId()));
        }

        private void processEvent(Object event) {
            try {
                if (event instanceof WatcherSetEventPair) {
                    // each watcher will process the event
                    WatcherSetEventPair pair = (WatcherSetEventPair) event;
                    for (Watcher watcher : pair.watchers) {
                        try {
                            watcher.process(pair.event);
                        } catch (Throwable t) {
                            LOG.error("Error while calling watcher ", t);
                        }
                    }
                } else {
                    Packet p = (Packet) event;
                    int rc = 0;
                    String clientPath = p.clientPath;
                    if (p.replyHeader.getErr() != 0) {
                        rc = p.replyHeader.getErr();
                    }
                    if (p.cb == null) {
                        LOG.warn("Somehow a null cb got to EventThread!");
                    } else if (p.response instanceof ExistsResponse
                            || p.response instanceof SetDataResponse
                            || p.response instanceof SetACLResponse) {
                        StatCallback cb = (StatCallback) p.cb;
                        if (rc == 0) {
                            if (p.response instanceof ExistsResponse) {
                                cb.processResult(rc, clientPath, p.ctx,
                                        ((ExistsResponse) p.response)
                                                .getStat());
                            } else if (p.response instanceof SetDataResponse) {
                                cb.processResult(rc, clientPath, p.ctx,
                                        ((SetDataResponse) p.response)
                                                .getStat());
                            } else if (p.response instanceof SetACLResponse) {
                                cb.processResult(rc, clientPath, p.ctx,
                                        ((SetACLResponse) p.response)
                                                .getStat());
                            }
                        } else {
                            cb.processResult(rc, clientPath, p.ctx, null);
                        }
                    } else if (p.response instanceof GetDataResponse) {
                        DataCallback cb = (DataCallback) p.cb;
                        GetDataResponse rsp = (GetDataResponse) p.response;
                        if (rc == 0) {
                            cb.processResult(rc, clientPath, p.ctx, rsp
                                    .getData(), rsp.getStat());
                        } else {
                            cb.processResult(rc, clientPath, p.ctx, null,
                                    null);
                        }
                    } else if (p.response instanceof GetACLResponse) {
                        ACLCallback cb = (ACLCallback) p.cb;
                        GetACLResponse rsp = (GetACLResponse) p.response;
                        if (rc == 0) {
                            cb.processResult(rc, clientPath, p.ctx, rsp
                                    .getAcl(), rsp.getStat());
                        } else {
                            cb.processResult(rc, clientPath, p.ctx, null,
                                    null);
                        }
                    } else if (p.response instanceof GetChildrenResponse) {
                        ChildrenCallback cb = (ChildrenCallback) p.cb;
                        GetChildrenResponse rsp = (GetChildrenResponse) p.response;
                        if (rc == 0) {
                            cb.processResult(rc, clientPath, p.ctx, rsp
                                    .getChildren());
                        } else {
                            cb.processResult(rc, clientPath, p.ctx, null);
                        }
                    } else if (p.response instanceof GetChildren2Response) {
                        Children2Callback cb = (Children2Callback) p.cb;
                        GetChildren2Response rsp = (GetChildren2Response) p.response;
                        if (rc == 0) {
                            cb.processResult(rc, clientPath, p.ctx, rsp
                                    .getChildren(), rsp.getStat());
                        } else {
                            cb.processResult(rc, clientPath, p.ctx, null, null);
                        }
                    } else if (p.response instanceof CreateResponse) {
                        StringCallback cb = (StringCallback) p.cb;
                        CreateResponse rsp = (CreateResponse) p.response;
                        if (rc == 0) {
                            cb.processResult(rc, clientPath, p.ctx,
                                    (chrootPath == null
                                            ? rsp.getPath()
                                            : rsp.getPath()
                                            .substring(chrootPath.length())));
                        } else {
                            cb.processResult(rc, clientPath, p.ctx, null);
                        }
                    } else if (p.response instanceof MultiResponse) {
                        MultiCallback cb = (MultiCallback) p.cb;
                        MultiResponse rsp = (MultiResponse) p.response;
                        if (rc == 0) {
                            List<OpResult> results = rsp.getResultList();
                            int newRc = rc;
                            for (OpResult result : results) {
                                if (result instanceof ErrorResult
                                        && KeeperException.Code.OK.intValue()
                                        != (newRc = ((ErrorResult) result).getErr())) {
                                    break;
                                }
                            }
                            cb.processResult(newRc, clientPath, p.ctx, results);
                        } else {
                            cb.processResult(rc, clientPath, p.ctx, null);
                        }
                    } else if (p.cb instanceof VoidCallback) {
                        VoidCallback cb = (VoidCallback) p.cb;
                        cb.processResult(rc, clientPath, p.ctx);
                    }
                }
            } catch (Throwable t) {
                LOG.error("Caught unexpected throwable", t);
            }
        }
    }

    private void finishPacket(Packet p) {
        if (p.watchRegistration != null) {
            p.watchRegistration.register(p.replyHeader.getErr());
        }

        // 同步callback为null
        if (p.cb == null) {
            synchronized (p) {
                // finished设置为true
                p.finished = true;
                // 唤醒当前packet
                System.out.println("时间: " + System.nanoTime() + ", 客户端获得了服务器的返回结果, 唤醒客户端等待的请求, p: " + p);
                p.notifyAll();
            }
        } else {
            // 操作异步通知, 添加监听器
            p.finished = true;
            eventThread.queuePacket(p);
        }
    }

    private void conLossPacket(Packet p) {
        if (p.replyHeader == null) {
            return;
        }
        switch (state) {
            case AUTH_FAILED:
                p.replyHeader.setErr(KeeperException.Code.AUTHFAILED.intValue());
                break;
            case CLOSED:
                p.replyHeader.setErr(KeeperException.Code.SESSIONEXPIRED.intValue());
                break;
            default:
                p.replyHeader.setErr(KeeperException.Code.CONNECTIONLOSS.intValue());
        }
        finishPacket(p);
    }

    private volatile long lastZxid;

    public long getLastZxid() {
        return lastZxid;
    }

    static class EndOfStreamException extends IOException {
        private static final long serialVersionUID = -5438877188796231422L;

        public EndOfStreamException(String msg) {
            super(msg);
        }

        @Override
        public String toString() {
            return "EndOfStreamException: " + getMessage();
        }
    }

    private static class SessionTimeoutException extends IOException {
        private static final long serialVersionUID = 824482094072071178L;

        public SessionTimeoutException(String msg) {
            super(msg);
        }
    }

    private static class SessionExpiredException extends IOException {
        private static final long serialVersionUID = -1388816932076193249L;

        public SessionExpiredException(String msg) {
            super(msg);
        }
    }

    private static class RWServerFoundException extends IOException {
        private static final long serialVersionUID = 90431199887158758L;

        public RWServerFoundException(String msg) {
            super(msg);
        }
    }

    public static final int packetLen = Integer.getInteger("jute.maxbuffer",
            4096 * 1024);

    /**
     * This class services the outgoing request queue and generates the heart
     * beats. It also spawns the ReadThread.
     *
     * 在我们对请求信息进行封装和序列化后, ZooKeeper不会立刻就将一个请求信息通过网络直接发送给服务端.
     * 而是通过将请求信息添加到队列中,之后通过 sendThread 线程类来处理相关的请求发送等操作.
     * 这种方式很像生产者和消费者模式,我们将请求信息准备好,并添加到队列中的操作相当于生成者,
     * 而sendThread线程从队列中取出要发送的请求信息,并发送给服务端相当于消费者操作
     *
     * SendThread类是一个线程类,其本质是一个 I/O 调度线程,它的作用就是用来管理操作客户端和服务端的网络I/O等.
     * 在ZooKeeper服务的运行过程中,SendThread类的作用除了上面提到的负责将客户端的请求发送给服务端外,
     * 另一个作用是发送客户端是否存活的心跳检查,SendThread类负责定期向服务端发送PING包来实现心跳检查
     */
    class SendThread extends ZooKeeperThread {
        private long lastPingSentNs;

        /**
         * 与服务器交互的核心类
         */
        private final ClientCnxnSocket clientCnxnSocket;

        private Random r = new Random(System.nanoTime());
        private boolean isFirstConnect = true;

        void readResponse(ByteBuffer incomingBuffer) throws IOException {
            ByteBufferInputStream bbis = new ByteBufferInputStream(incomingBuffer);
            BinaryInputArchive bbia = BinaryInputArchive.getArchive(bbis);
            ReplyHeader replyHdr = new ReplyHeader();

            replyHdr.deserialize(bbia, "header");
            // -2是Ping
            if (replyHdr.getXid() == -2) {
                // -2 is the xid for pings
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Got ping response for sessionid: 0x"
                            + Long.toHexString(sessionId)
                            + " after "
                            + ((System.nanoTime() - lastPingSentNs) / 1000000)
                            + "ms");
                }
                return;
            }
            // -4是认证消息
            if (replyHdr.getXid() == -4) {
                // -4 is the xid for AuthPacket               
                if (replyHdr.getErr() == KeeperException.Code.AUTHFAILED.intValue()) {
                    state = States.AUTH_FAILED;
                    eventThread.queueEvent(new WatchedEvent(Watcher.Event.EventType.None,
                            Watcher.Event.KeeperState.AuthFailed, null));
                }
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Got auth sessionid:0x"
                            + Long.toHexString(sessionId));
                }
                return;
            }
            // -1是Watch通知
            if (replyHdr.getXid() == -1) {
                // -1 means notification
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Got notification sessionid:0x"
                            + Long.toHexString(sessionId));
                }
                WatcherEvent event = new WatcherEvent();
                // 反序列化
                event.deserialize(bbia, "response");

                // convert from a server path to a client path
                if (chrootPath != null) {
                    String serverPath = event.getPath();
                    if (serverPath.compareTo(chrootPath) == 0)
                        event.setPath("/");
                    else if (serverPath.length() > chrootPath.length())
                        event.setPath(serverPath.substring(chrootPath.length()));
                    else {
                        LOG.warn("Got server path " + event.getPath()
                                + " which is too short for chroot path "
                                + chrootPath);
                    }
                }

                WatchedEvent we = new WatchedEvent(event);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Got " + we + " for sessionid 0x"
                            + Long.toHexString(sessionId));
                }

                // 添加到eventThread处理的队列中
                eventThread.queueEvent(we);
                return;
            }

            // If SASL authentication is currently in progress, construct and
            // send a response packet immediately, rather than queuing a
            // response as with other packets.
            if (clientTunneledAuthenticationInProgress()) {
                GetSASLRequest request = new GetSASLRequest();
                request.deserialize(bbia, "token");
                zooKeeperSaslClient.respondToServer(request.getToken(),
                        ClientCnxn.this);
                return;
            }

            // 正常业务消息走到这里
            Packet packet;
            synchronized (pendingQueue) {
                if (pendingQueue.size() == 0) {
                    throw new IOException("Nothing in the queue, but got " + replyHdr.getXid());
                }
                packet = pendingQueue.remove();
            }
            /*
             * Since requests are processed in order, we better get a response
             * to the first request!
             */
            try {
                // 两个消息ID不一致
                if (packet.requestHeader.getXid() != replyHdr.getXid()) {
                    packet.replyHeader.setErr(KeeperException.Code.CONNECTIONLOSS.intValue());
                    throw new IOException("Xid out of order. Got Xid "
                            + replyHdr.getXid() + " with err " +
                            +replyHdr.getErr() +
                            " expected Xid "
                            + packet.requestHeader.getXid()
                            + " for a packet with details: "
                            + packet);
                }

                packet.replyHeader.setXid(replyHdr.getXid());
                packet.replyHeader.setErr(replyHdr.getErr());
                packet.replyHeader.setZxid(replyHdr.getZxid());

                if (replyHdr.getZxid() > 0) {
                    // 更新lastZxid
                    lastZxid = replyHdr.getZxid();
                }
                if (packet.response != null && replyHdr.getErr() == 0) {
                    // 响应消息反序列化
                    packet.response.deserialize(bbia, "response");
                }

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Reading reply sessionid:0x"
                            + Long.toHexString(sessionId) + ", packet:: " + packet);
                }
            } finally {
                // 这里面会唤醒在等待结果的请求
                finishPacket(packet);
            }
        }

        SendThread(ClientCnxnSocket clientCnxnSocket) {
            super(makeThreadName("-SendThread()"));
            state = States.CONNECTING;
            this.clientCnxnSocket = clientCnxnSocket;
            setDaemon(true);
        }

        // TODO: can not name this method getState since Thread.getState()
        // already exists
        // It would be cleaner to make class SendThread an implementation of
        // Runnable

        /**
         * Used by ClientCnxnSocket
         *
         * @return
         */
        ZooKeeper.States getZkState() {
            return state;
        }

        ClientCnxnSocket getClientCnxnSocket() {
            return clientCnxnSocket;
        }

        void primeConnection() throws IOException {
            LOG.info("Socket connection established to " + clientCnxnSocket.getRemoteSocketAddress() + ", initiating session");
            // 表明不是第一次连接了
            isFirstConnect = false;
            // 如果之前连的是RwServer, 则带上sessionId, 否则传0
            long sessId = (seenRwServerBefore) ? sessionId : 0;
            /**
             * 构造ConnectRequest
             * 带上sessionTimeOut(new ZooKeeper时可以指定该参数, 服务器收到后根据自身配置和这个参数一起决定会话超时时间)
             */
            ConnectRequest conReq = new ConnectRequest(0, lastZxid, sessionTimeout, sessId, sessionPasswd);
            synchronized (outgoingQueue) {
                // We add backwards since we are pushing into the front
                // Only send if there's a pending watch
                // TODO: here we have the only remaining use of zooKeeper in
                // this class. It's to be eliminated!
                if (!disableAutoWatchReset) {
                    List<String> dataWatches = zooKeeper.getDataWatches();
                    List<String> existWatches = zooKeeper.getExistWatches();
                    List<String> childWatches = zooKeeper.getChildWatches();
                    if (!dataWatches.isEmpty()
                            || !existWatches.isEmpty() || !childWatches.isEmpty()) {

                        Iterator<String> dataWatchesIter = prependChroot(dataWatches).iterator();
                        Iterator<String> existWatchesIter = prependChroot(existWatches).iterator();
                        Iterator<String> childWatchesIter = prependChroot(childWatches).iterator();
                        long setWatchesLastZxid = lastZxid;

                        while (dataWatchesIter.hasNext()
                                || existWatchesIter.hasNext() || childWatchesIter.hasNext()) {
                            List<String> dataWatchesBatch = new ArrayList<String>();
                            List<String> existWatchesBatch = new ArrayList<String>();
                            List<String> childWatchesBatch = new ArrayList<String>();
                            int batchLength = 0;

                            // Note, we may exceed our max length by a bit when we add the last
                            // watch in the batch. This isn't ideal, but it makes the code simpler.
                            while (batchLength < SET_WATCHES_MAX_LENGTH) {
                                final String watch;
                                if (dataWatchesIter.hasNext()) {
                                    watch = dataWatchesIter.next();
                                    dataWatchesBatch.add(watch);
                                } else if (existWatchesIter.hasNext()) {
                                    watch = existWatchesIter.next();
                                    existWatchesBatch.add(watch);
                                } else if (childWatchesIter.hasNext()) {
                                    watch = childWatchesIter.next();
                                    childWatchesBatch.add(watch);
                                } else {
                                    break;
                                }
                                batchLength += watch.length();
                            }

                            SetWatches sw = new SetWatches(setWatchesLastZxid,
                                    dataWatchesBatch,
                                    existWatchesBatch,
                                    childWatchesBatch);
                            RequestHeader h = new RequestHeader();
                            h.setType(ZooDefs.OpCode.setWatches);
                            h.setXid(-8);
                            Packet packet = new Packet(h, new ReplyHeader(), sw, null, null);
                            outgoingQueue.addFirst(packet);
                        }
                    }
                }

                for (AuthData id : authInfo) {
                    outgoingQueue.addFirst(new Packet(new RequestHeader(-4,
                            OpCode.auth), null, new AuthPacket(0, id.scheme,
                            id.data), null, null));
                }

                // 加入一个连接建立请求Packet
                System.out.println("时间: " + System.nanoTime() + ", 客户端与服务器建立了TCP连接, " +
                                             "往outgoingQueue里面放入了一个ConnectRequest请求, 尝试和ZooKeeperServer建立连接");
                outgoingQueue.addFirst(new Packet(null, null, conReq, null, null, readOnly));
            }
            // 完了之后通道可读可写
            clientCnxnSocket.enableReadWriteOnly();
            if (LOG.isDebugEnabled()) {
                LOG.debug("Session establishment request sent on "
                        + clientCnxnSocket.getRemoteSocketAddress());
            }
        }

        private List<String> prependChroot(List<String> paths) {
            if (chrootPath != null && !paths.isEmpty()) {
                for (int i = 0; i < paths.size(); ++i) {
                    String clientPath = paths.get(i);
                    String serverPath;
                    // handle clientPath = "/"
                    if (clientPath.length() == 1) {
                        serverPath = chrootPath;
                    } else {
                        serverPath = chrootPath + clientPath;
                    }
                    paths.set(i, serverPath);
                }
            }
            return paths;
        }

        private void sendPing() {
            // 获取系统当前时间
            lastPingSentNs = System.nanoTime();
            // 之后设置请求头的操作类型为心跳检查操作OpCode.ping
            // ZooKeeper服务端收到Ping操作的请求时,会根据服务端的当前时间重置与客户端的Session时间,更新该会话的请求延迟时间等.进而保持客户端与服务端连接状态
            RequestHeader h = new RequestHeader(-2, OpCode.ping);
            // 最后将请求信息封装成Packet对象发送给ZooKeeper服务端
            queuePacket(h, null, null, null, null, null, null, null, null);
        }

        private InetSocketAddress rwServerAddress = null;

        private final static int minPingRwTimeout = 100;

        private final static int maxPingRwTimeout = 60000;

        private int pingRwTimeout = minPingRwTimeout;

        // Set to true if and only if constructor of ZooKeeperSaslClient
        // throws a LoginException: see startConnect() below.
        private boolean saslLoginFailed = false;

        private void startConnect(InetSocketAddress addr) throws IOException {
            // initializing it for new connection
            saslLoginFailed = false;
            state = States.CONNECTING;

            setName(getName().replaceAll("\\(.*\\)", "(" + addr.getHostName() + ":" + addr.getPort() + ")"));
            if (ZooKeeperSaslClient.isEnabled()) {
                try {
                    String principalUserName = System.getProperty(
                            ZK_SASL_CLIENT_USERNAME, "zookeeper");
                    zooKeeperSaslClient =
                            new ZooKeeperSaslClient(
                                    principalUserName + "/" + addr.getHostName());
                } catch (LoginException e) {
                    // An authentication error occurred when the SASL client tried to initialize:
                    // for Kerberos this means that the client failed to authenticate with the KDC.
                    // This is different from an authentication error that occurs during communication
                    // with the Zookeeper server, which is handled below.
                    LOG.warn("SASL configuration failed: " + e + " Will continue connection to Zookeeper server without "
                            + "SASL authentication, if Zookeeper server allows it.");
                    eventThread.queueEvent(new WatchedEvent(
                            Watcher.Event.EventType.None,
                            Watcher.Event.KeeperState.AuthFailed, null));
                    saslLoginFailed = true;
                }
            }
            logStartConnect(addr);

            // 连接服务器
            clientCnxnSocket.connect(addr);
        }

        private void logStartConnect(InetSocketAddress addr) {
            String msg = "Opening socket connection to server " + addr;
            if (zooKeeperSaslClient != null) {
                msg += ". " + zooKeeperSaslClient.getConfigStatus();
            }
            LOG.info(msg);
        }

        private static final String RETRY_CONN_MSG =
                ", closing socket connection and attempting reconnect";

        @Override
        public void run() {
            clientCnxnSocket.introduce(this, sessionId);
            clientCnxnSocket.updateNow();
            clientCnxnSocket.updateLastSendAndHeard();
            int to;
            long lastPingRwServer = Time.currentElapsedTime();
            final int MAX_SEND_PING_INTERVAL = 10000; //10 seconds
            InetSocketAddress serverAddress = null;
            /**
             * 这边是大的一个while死循环
             */
            while (state.isAlive()) {
                try {
                    /**
                     * 如果socket没有连接, 判断isConnected是根据sockKey != null, 在ClientCnxnSocketNIO.registerAndConnect方法中客户端尝试与服务器发起TCP连接
                     * 这里面之后socketKey就被赋值了
                     */
                    if (!clientCnxnSocket.isConnected()) {
                        // 判断是不是第一次连接
                        if (!isFirstConnect) {
                            try {
                                // 不是第一次连接, 再次连接时会睡一会
                                Thread.sleep(r.nextInt(1000));
                            } catch (InterruptedException e) {
                                LOG.warn("Unexpected exception", e);
                            }
                        }
                        /**
                         * don't re-establish connection if we are closing
                         * 如果连接关闭, 跳出循环
                         */
                        if (closing || !state.isAlive()) {
                            break;
                        }
                        // 读写服务端地址, 暂时没有就从hostProvider中取一个
                        if (rwServerAddress != null) {
                            serverAddress = rwServerAddress;
                            rwServerAddress = null;
                        } else {
                            // 获取一个服务端地址
                            serverAddress = hostProvider.next(1000);
                        }
                        // 连接服务端
                        System.out.println("时间: " + System.nanoTime() + ", 客户端准备与服务器建立TCP连接");
                        startConnect(serverAddress);
                        clientCnxnSocket.updateLastSendAndHeard();
                    }

                    // 连接成功
                    if (state.isConnected()) {
                        // determine whether we need to send an AuthFailed event.
                        if (zooKeeperSaslClient != null) {
                            boolean sendAuthEvent = false;
                            if (zooKeeperSaslClient.getSaslState() == ZooKeeperSaslClient.SaslState.INITIAL) {
                                try {
                                    zooKeeperSaslClient.initialize(ClientCnxn.this);
                                } catch (SaslException e) {
                                    LOG.error("SASL authentication with Zookeeper Quorum member failed: " + e);
                                    state = States.AUTH_FAILED;
                                    sendAuthEvent = true;
                                }
                            }
                            KeeperState authState = zooKeeperSaslClient.getKeeperState();
                            if (authState != null) {
                                if (authState == KeeperState.AuthFailed) {
                                    // An authentication error occurred during authentication with the Zookeeper Server.
                                    state = States.AUTH_FAILED;
                                    sendAuthEvent = true;
                                } else {
                                    if (authState == KeeperState.SaslAuthenticated) {
                                        sendAuthEvent = true;
                                    }
                                }
                            }

                            if (sendAuthEvent == true) {
                                eventThread.queueEvent(new WatchedEvent(
                                        Watcher.Event.EventType.None,
                                        authState, null));
                            }
                        }
                        // 如果连接成功, 看是否超过了readTime
                        to = readTimeout - clientCnxnSocket.getIdleRecv();
                    } else {
                        to = connectTimeout - clientCnxnSocket.getIdleRecv();
                    }

                    // 超时了则抛异常SessionTimeoutException, 但这个异常会被上层catch住, 尝试重连
                    if (to <= 0) {
                        String warnInfo;
                        warnInfo = "Client session timed out, have not heard from server in "
                                + clientCnxnSocket.getIdleRecv()
                                + "ms"
                                + " for sessionid 0x"
                                + Long.toHexString(sessionId);
                        LOG.warn(warnInfo);
                        throw new SessionTimeoutException(warnInfo);
                    }
                    // 会话保活
                    if (state.isConnected()) {
                        // 1000(1 second) is to prevent race condition missing to send the second ping
                        // also make sure not to send too many pings when readTimeout is small
                        // 下一个ping的时间
                        int timeToNextPing = readTimeout / 2 - clientCnxnSocket.getIdleSend() - ((clientCnxnSocket.getIdleSend() > 1000) ? 1000 : 0);
                        //send a ping request either time is due or no packet sent out within MAX_SEND_PING_INTERVAL
                        if (timeToNextPing <= 0 || clientCnxnSocket.getIdleSend() > MAX_SEND_PING_INTERVAL) {
                            sendPing();
                            clientCnxnSocket.updateLastSend();
                        } else {
                            if (timeToNextPing < to) {
                                to = timeToNextPing;
                            }
                        }
                    }

                    /**
                     * If we are in read-only mode, seek for read/write server
                     * client连接了只读的server时, 不断根据hostProvider找到一个可读写的server
                     */
                    if (state == States.CONNECTEDREADONLY) {
                        long now = Time.currentElapsedTime();
                        int idlePingRwServer = (int) (now - lastPingRwServer);
                        if (idlePingRwServer >= pingRwTimeout) {
                            lastPingRwServer = now;
                            idlePingRwServer = 0;
                            pingRwTimeout = Math.min(2 * pingRwTimeout, maxPingRwTimeout);
                            // 选择一个server向其发送请求询问是否为readOnly
                            pingRwServer();
                        }
                        to = Math.min(to, pingRwTimeout - idlePingRwServer);
                    }

                    /**
                     * 1) 客户端启动时向服务器发起建立TCP连接请求之后会走到这里, 在收到服务器响应之后尝试在这条TCP连接上面向服务器发起ConnectRequest请求
                     * 2) 发起ConnectRequest请求时会构造一个请求放到outGoingQueue里面等待sendThread处理
                     * 3) 然后外面是一个大的while循环, 又走到这里, 这时会从outGoingQueue里面取出刚刚放入的ConnectRequest请求
                     */
                    clientCnxnSocket.doTransport(to, pendingQueue, outgoingQueue, ClientCnxn.this);
                } catch (Throwable e) {
                    // 已经关闭就直接退出了, 否则catch住异常之后还可以进入大循环中尝试再次连接
                    if (closing) {
                        if (LOG.isDebugEnabled()) {
                            // closing so this is expected
                            LOG.debug("An exception was thrown while closing send thread for session 0x"
                                    + Long.toHexString(getSessionId())
                                    + " : " + e.getMessage());
                        }
                        break;
                    } else {
                        // this is ugly, you have a better way speak up
                        if (e instanceof SessionExpiredException) {
                            LOG.info(e.getMessage() + ", closing socket connection");
                        } else if (e instanceof SessionTimeoutException) {
                            LOG.info(e.getMessage() + RETRY_CONN_MSG);
                        } else if (e instanceof EndOfStreamException) {
                            LOG.info(e.getMessage() + RETRY_CONN_MSG);
                        } else if (e instanceof RWServerFoundException) {
                            LOG.info(e.getMessage());
                        } else if (e instanceof SocketException) {
                            LOG.info("Socket error occurred: {}: {}", serverAddress, e.getMessage());
                        } else {
                            LOG.warn("Session 0x{} for server {}, unexpected error{}",
                                    Long.toHexString(getSessionId()),
                                    serverAddress,
                                    RETRY_CONN_MSG,
                                    e);
                        }

                        // 一些清理动作, 关闭socket连接, 清理queue队列的里面的数据
                        cleanup();

                        if (state.isAlive()) {
                            // catch住上述异常之后, 放入一个Disconnected类型的Event, 看了下不会做处理直接return掉了
                            eventThread.queueEvent(new WatchedEvent(
                                                   Event.EventType.None,
                                                   Event.KeeperState.Disconnected,
                                              null));
                        }
                        clientCnxnSocket.updateNow();
                        clientCnxnSocket.updateLastSendAndHeard();
                    }
                }
            }
            cleanup();
            clientCnxnSocket.close();
            if (state.isAlive()) {
                eventThread.queueEvent(new WatchedEvent(Event.EventType.None,
                        Event.KeeperState.Disconnected, null));
            }
            ZooTrace.logTraceMessage(LOG, ZooTrace.getTextTraceLevel(),
                    "SendThread exited loop for session: 0x"
                            + Long.toHexString(getSessionId()));
        }

        private void pingRwServer() throws RWServerFoundException {
            String result = null;
            InetSocketAddress addr = hostProvider.next(0);
            LOG.info("Checking server " + addr + " for being r/w." + " Timeout " + pingRwTimeout);

            Socket sock = null;
            BufferedReader br = null;
            try {
                sock = new Socket(addr.getHostName(), addr.getPort());
                sock.setSoLinger(false, -1);
                sock.setSoTimeout(1000);
                sock.setTcpNoDelay(true);

                // 发送isReadOnly请求
                sock.getOutputStream().write("isro".getBytes());
                sock.getOutputStream().flush();
                sock.shutdownOutput();

                // 获取返回结果
                br = new BufferedReader(new InputStreamReader(sock.getInputStream()));
                result = br.readLine();
            } catch (ConnectException e) {
                // ignore, this just means server is not up
            } catch (IOException e) {
                // some unexpected error, warn about it
                LOG.warn("Exception while seeking for r/w server " +
                        e.getMessage(), e);
            } finally {
                if (sock != null) {
                    try {
                        sock.close();
                    } catch (IOException e) {
                        LOG.warn("Unexpected exception", e);
                    }
                }
                if (br != null) {
                    try {
                        br.close();
                    } catch (IOException e) {
                        LOG.warn("Unexpected exception", e);
                    }
                }
            }

            // 判断是不是rw, 如果是的话抛出RWServerFoundException会被外层捕获, 然后继续大的while循环尝试连接rwServerAddress
            if ("rw".equals(result)) {
                pingRwTimeout = minPingRwTimeout;
                // save the found address so that it's used during the next
                // connection attempt
                rwServerAddress = addr;
                throw new RWServerFoundException("Majority server found at " + addr.getHostName() + ":" + addr.getPort());
            }
        }

        private void cleanup() {
            // 将sockKey置null, 关闭与服务器的Socket连接
            clientCnxnSocket.cleanup();
            synchronized (pendingQueue) {
                for (Packet p : pendingQueue) {
                    conLossPacket(p);
                }
                pendingQueue.clear();
            }
            synchronized (outgoingQueue) {
                for (Packet p : outgoingQueue) {
                    conLossPacket(p);
                }
                outgoingQueue.clear();
            }
        }

        /**
         * Callback invoked by the ClientCnxnSocket once a connection has been
         * established.
         *
         * @param _negotiatedSessionTimeout
         * @param _sessionId
         * @param _sessionPasswd
         * @param isRO
         * @throws IOException
         */
        void onConnected(int _negotiatedSessionTimeout, long _sessionId,
                         byte[] _sessionPasswd, boolean isRO) throws IOException {
            negotiatedSessionTimeout = _negotiatedSessionTimeout;

            // 理解是 <= 0表明会话超期了
            if (negotiatedSessionTimeout <= 0) {

                state = States.CLOSED;

                eventThread.queueEvent(new WatchedEvent(
                        Watcher.Event.EventType.None,
                        Watcher.Event.KeeperState.Expired, null));
                eventThread.queueEventOfDeath();

                String warnInfo;
                warnInfo = "Unable to reconnect to ZooKeeper service, session 0x"
                        + Long.toHexString(sessionId) + " has expired";
                LOG.warn(warnInfo);
                throw new SessionExpiredException(warnInfo);
            }
            if (!readOnly && isRO) {
                LOG.error("Read/write client got connected to read-only server");
            }
            readTimeout = negotiatedSessionTimeout * 2 / 3;
            connectTimeout = negotiatedSessionTimeout / hostProvider.size();

            // 通知地址管理器HostProvider当前成功连接的服务器地址
            hostProvider.onConnected();

            sessionId = _sessionId;
            sessionPasswd = _sessionPasswd;

            // 更新state字段
            state = (isRO) ? States.CONNECTEDREADONLY : States.CONNECTED;
            System.out.println("时间: " + System.nanoTime() + ", 客户端与ZooKeeper服务连接建立完毕, 更新连接状态为: " + state);

            seenRwServerBefore |= !isRO;
            LOG.info("Session establishment complete on server "
                    + clientCnxnSocket.getRemoteSocketAddress()
                    + ", sessionid = 0x" + Long.toHexString(sessionId)
                    + ", negotiated timeout = " + negotiatedSessionTimeout
                    + (isRO ? " (READ-ONLY mode)" : ""));
            KeeperState eventState = (isRO) ? KeeperState.ConnectedReadOnly : KeeperState.SyncConnected;
            System.out.println("时间: " + System.nanoTime() + ", 连接建立完毕之后, 客户端向eventThread发送一个WatcherEvent: "
                    + new WatchedEvent(Watcher.Event.EventType.None, eventState, null));
            /**
             * 为了能够让上层应用感知到会话的成功创建, SendThread会生成一个事件SyncConnected-None, 代表客户端与服务器会话创建成功.
             * 将该事件传递给EventThread线程, EventThread线程收到该事件之后, 会从ClientWatcherManager管理器中查询出对应的Watcher,
             * 针对SyncConnected-None事件, 直接找出默认的Watcher, 然后将其放到EventThread的waitingThreads队列中去. EventThread
             * 不断地从waitingEvents队列中取出待处理的Watcher对象, 然后直接调用该对象的process接口方法, 已达到触发Watcher的目的
             *
             */
            eventThread.queueEvent(new WatchedEvent(Watcher.Event.EventType.None, eventState, null));
        }

        void close() {
            state = States.CLOSED;
            clientCnxnSocket.wakeupCnxn();
        }

        void testableCloseSocket() throws IOException {
            clientCnxnSocket.testableCloseSocket();
        }

        public boolean clientTunneledAuthenticationInProgress() {
            // 1. SASL client is disabled.
            if (!ZooKeeperSaslClient.isEnabled()) {
                return false;
            }

            // 2. SASL login failed.
            if (saslLoginFailed == true) {
                return false;
            }

            // 3. SendThread has not created the authenticating object yet,
            // therefore authentication is (at the earliest stage of being) in progress.
            if (zooKeeperSaslClient == null) {
                return true;
            }

            // 4. authenticating object exists, so ask it for its progress.
            return zooKeeperSaslClient.clientTunneledAuthenticationInProgress();
        }

        public void sendPacket(Packet p) throws IOException {
            clientCnxnSocket.sendPacket(p);
        }
    }

    /**
     * Shutdown the send/event threads. This method should not be called
     * directly - rather it should be called as part of close operation. This
     * method is primarily here to allow the tests to verify disconnection
     * behavior.
     */
    public void disconnect() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Disconnecting client for session: 0x"
                    + Long.toHexString(getSessionId()));
        }

        sendThread.close();
        eventThread.queueEventOfDeath();
    }

    /**
     * Close the connection, which includes; send session disconnect to the
     * server, shutdown the send/event threads.
     *
     * @throws IOException
     */
    public void close() throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Closing client for session: 0x"
                    + Long.toHexString(getSessionId()));
        }

        try {
            RequestHeader h = new RequestHeader();
            h.setType(ZooDefs.OpCode.closeSession);

            submitRequest(h, null, null, null);
        } catch (InterruptedException e) {
            // ignore, close the send/event threads
        } finally {
            disconnect();
        }
    }

    private int xid = 1;

    private volatile States state = States.NOT_CONNECTED;

    /*
     * getXid() is called externally by ClientCnxnNIO::doIO() when packets are sent from the outgoingQueue to
     * the server. Thus, getXid() must be public.
     */
    synchronized public int getXid() {
        return xid++;
    }

    public ReplyHeader submitRequest(RequestHeader h, Record request,
                                     Record response, WatchRegistration watchRegistration)
            throws InterruptedException {
        ReplyHeader r = new ReplyHeader();
        // callback cb为null, 如果有是异步回调, 为null是同步
        Packet packet = queuePacket(h, r, request, response, null, null, null, null, watchRegistration);
        // 死循环等待结果
        synchronized (packet) {
            // 在readResponse的finishPacket方法中, 获取到返回结果后会将finished置为true, 并唤醒等待在packet上面的线程
            while (!packet.finished) {
                packet.wait();
            }
        }
        return r;
    }

    public void enableWrite() {
        sendThread.getClientCnxnSocket().enableWrite();
    }

    public void sendPacket(Record request, Record response, AsyncCallback cb, int opCode)
            throws IOException {
        // Generate Xid now because it will be sent immediately,
        // by call to sendThread.sendPacket() below.
        int xid = getXid();
        RequestHeader h = new RequestHeader();
        h.setXid(xid);
        h.setType(opCode);

        ReplyHeader r = new ReplyHeader();
        r.setXid(xid);

        Packet p = new Packet(h, r, request, response, null, false);
        p.cb = cb;
        sendThread.sendPacket(p);
    }

    Packet queuePacket(RequestHeader h, ReplyHeader r, Record request,
                       Record response, AsyncCallback cb, String clientPath,
                       String serverPath, Object ctx, WatchRegistration watchRegistration) {
        Packet packet = null;

        // Note that we do not generate the Xid for the packet yet. It is
        // generated later at send-time, by an implementation of ClientCnxnSocket::doIO(),
        // where the packet is actually sent.
        synchronized (outgoingQueue) {
            packet = new Packet(h, r, request, response, watchRegistration);
            packet.cb = cb;
            packet.ctx = ctx;
            packet.clientPath = clientPath;
            packet.serverPath = serverPath;
            if (!state.isAlive() || closing) {
                conLossPacket(packet);
            } else {
                // If the client is asking to close the session then mark as closing
                if (h.getType() == OpCode.closeSession) {
                    // 置为终止, while大循环感知到后就会退出大循环
                    closing = true;
                }
                // new了一个Packet放到outgoingQueue
                outgoingQueue.add(packet);
            }
        }
        sendThread.getClientCnxnSocket().wakeupCnxn();
        return packet;
    }

    public void addAuthInfo(String scheme, byte auth[]) {
        if (!state.isAlive()) {
            return;
        }
        authInfo.add(new AuthData(scheme, auth));
        queuePacket(new RequestHeader(-4, OpCode.auth), null,
                new AuthPacket(0, scheme, auth), null, null, null, null,
                null, null);
    }

    States getState() {
        return state;
    }
}

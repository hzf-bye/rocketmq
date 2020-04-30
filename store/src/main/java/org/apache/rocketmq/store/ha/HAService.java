/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.store.ha;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.store.CommitLog;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.config.MessageStoreConfig;

/**
 * RocketMq主从同步核心实现类
 *
 * HA的实现原理：
 * 1、主服务器启动，并在特定的端口监听从服务器的连接
 * 2、从服务器主动连接主服务器，主服务器接收客户端的连接，并建立相关TCP连接。
 * 3、从服务器主动向主服务器发送待拉取消息的偏移量，主服务器解析消息请求并返回给从服务器。
 * 4、从服务器保存消息并继续发送新的消息同步请求。
 */
public class HAService {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    /**
     * 连接到主服务器的从服务器数量
     */
    private final AtomicInteger connectionCount = new AtomicInteger(0);

    private final List<HAConnection> connectionList = new LinkedList<>();

    private final AcceptSocketService acceptSocketService;

    private final DefaultMessageStore defaultMessageStore;

    private final WaitNotifyObject waitNotifyObject = new WaitNotifyObject();

    /**
     * 已经同步到从服务器的最大偏移量
     */
    private final AtomicLong push2SlaveMaxOffset = new AtomicLong(0);

    /**
     * 主从同步通知实现类
     */
    private final GroupTransferService groupTransferService;

    private final HAClient haClient;

    public HAService(final DefaultMessageStore defaultMessageStore) throws IOException {
        this.defaultMessageStore = defaultMessageStore;
        this.acceptSocketService =
            new AcceptSocketService(defaultMessageStore.getMessageStoreConfig().getHaListenPort());
        this.groupTransferService = new GroupTransferService();
        this.haClient = new HAClient();
    }

    public void updateMasterAddress(final String newAddr) {
        if (this.haClient != null) {
            this.haClient.updateMasterAddress(newAddr);
        }
    }

    public void putRequest(final CommitLog.GroupCommitRequest request) {
        this.groupTransferService.putRequest(request);
    }

    /**
     * @param masterPutWhere 当前消息的最后一个字节在commitlog中的偏移量
     */
    public boolean isSlaveOK(final long masterPutWhere) {
        //获取连接到主服务器的从服务器数量
        boolean result = this.connectionCount.get() > 0;
        /**
         * 有从服务器连接，且当前消息的最后一个字节在commitlog中的偏移量与同步到从服务器中的消息偏移量小于256M，则认为消息生产者有必要等待
         * 换句话说就是
         * 1. 当未配置从服务器是或者当前消息的最后一个字节在commitlog中的偏移量与同步到从服务器中的消息偏移量大于等于256M时认为从服务器挂了，则，没必要等待
         */
        result =
            result
                && ((masterPutWhere - this.push2SlaveMaxOffset.get()) < this.defaultMessageStore
                .getMessageStoreConfig().getHaSlaveFallbehindMax());
        return result;
    }

    public void notifyTransferSome(final long offset) {
        //循环更新已经同步到从服务器的最大偏移量，直到更新成功
        for (long value = this.push2SlaveMaxOffset.get(); offset > value; ) {
            boolean ok = this.push2SlaveMaxOffset.compareAndSet(value, offset);
            if (ok) {
                this.groupTransferService.notifyTransferSome();
                break;
            } else {
                value = this.push2SlaveMaxOffset.get();
            }
        }
    }

    public AtomicInteger getConnectionCount() {
        return connectionCount;
    }

    // public void notifyTransferSome() {
    // this.groupTransferService.notifyTransferSome();
    // }

    public void start() throws Exception {
        this.acceptSocketService.beginAccept();
        this.acceptSocketService.start();
        this.groupTransferService.start();
        this.haClient.start();
    }

    public void addConnection(final HAConnection conn) {
        synchronized (this.connectionList) {
            this.connectionList.add(conn);
        }
    }

    public void removeConnection(final HAConnection conn) {
        synchronized (this.connectionList) {
            this.connectionList.remove(conn);
        }
    }

    public void shutdown() {
        this.haClient.shutdown();
        this.acceptSocketService.shutdown(true);
        this.destroyConnections();
        this.groupTransferService.shutdown();
    }

    public void destroyConnections() {
        synchronized (this.connectionList) {
            for (HAConnection c : this.connectionList) {
                c.shutdown();
            }

            this.connectionList.clear();
        }
    }

    public DefaultMessageStore getDefaultMessageStore() {
        return defaultMessageStore;
    }

    public WaitNotifyObject getWaitNotifyObject() {
        return waitNotifyObject;
    }

    public AtomicLong getPush2SlaveMaxOffset() {
        return push2SlaveMaxOffset;
    }

    /**
     * Listens to slave connections to create {@link HAConnection}.
     * HA master端监听客户端连接实现类
     */
    class AcceptSocketService extends ServiceThread {
        /**
         * Broker服务监听套接字（本地ip + 端口号）
         * 端口号
         * @see MessageStoreConfig#haListenPort
         */
        private final SocketAddress socketAddressListen;
        /**
         * 服务器端Socket通道，基于NIO
         */
        private ServerSocketChannel serverSocketChannel;
        /**
         * 时间选择器，基于NIO
         */
        private Selector selector;

        public AcceptSocketService(final int port) {
            this.socketAddressListen = new InetSocketAddress(port);
        }

        /**
         * Starts listening to slave connections.
         *
         * @throws Exception If fails.
         *
         * 创建serverSocketChannel、创建selector、设置tcp reuseAddress、绑定监听端口号、
         * 设置为非阻塞模式、并注册OP_ACCEPT（一个ServerSocketChannel准备好接收新进入的连接称为“接收就绪”）
         */
        public void beginAccept() throws Exception {
            this.serverSocketChannel = ServerSocketChannel.open();
            this.selector = RemotingUtil.openSelector();
            this.serverSocketChannel.socket().setReuseAddress(true);
            this.serverSocketChannel.socket().bind(this.socketAddressListen);
            this.serverSocketChannel.configureBlocking(false);
            this.serverSocketChannel.register(this.selector, SelectionKey.OP_ACCEPT);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void shutdown(final boolean interrupt) {
            super.shutdown(interrupt);
            try {
                this.serverSocketChannel.close();
                this.selector.close();
            } catch (IOException e) {
                log.error("AcceptSocketService shutdown exception", e);
            }
        }

        /**
         * {@inheritDoc}
         *  刚方法是标准的基于NIO的服务端程式实例，选择器每1s处理一次连接就绪时间。
         *  接收事件就绪后，调用ServerSocketChannel的accept方法创建SocketChannel。
         *  然后为每一个连接创建一个HAConnection对象，该HAConnection将负责M-S数据
         *  同步逻辑。
         */
        @Override
        public void run() {
            log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    this.selector.select(1000);
                    Set<SelectionKey> selected = this.selector.selectedKeys();

                    if (selected != null) {
                        for (SelectionKey k : selected) {
                            //接收就绪
                            if ((k.readyOps() & SelectionKey.OP_ACCEPT) != 0) {
                                SocketChannel sc = ((ServerSocketChannel) k.channel()).accept();

                                if (sc != null) {
                                    HAService.log.info("HAService receive new connection, "
                                        + sc.socket().getRemoteSocketAddress());

                                    try {
                                        HAConnection conn = new HAConnection(HAService.this, sc);
                                        conn.start();
                                        HAService.this.addConnection(conn);
                                    } catch (Exception e) {
                                        log.error("new HAConnection exception", e);
                                        sc.close();
                                    }
                                }
                            } else {
                                log.warn("Unexpected ops in select " + k.readyOps());
                            }
                        }

                        selected.clear();
                    }
                } catch (Exception e) {
                    log.error(this.getServiceName() + " service has exception.", e);
                }
            }

            log.info(this.getServiceName() + " service end");
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String getServiceName() {
            return AcceptSocketService.class.getSimpleName();
        }
    }

    /**
     * GroupTransferService Service
     * 主从同步通知实现类
     *
     * GroupTransferService的职责是负责当同步复制结束后，通知由于等待HA同步结果而阻塞的发送者线程
     * {@link CommitLog#handleHA(org.apache.rocketmq.store.AppendMessageResult, org.apache.rocketmq.store.PutMessageResult, org.apache.rocketmq.common.message.MessageExt)}
     * 判断主从同步是否完成的依据是Slave中已经成功复制的最大偏移量是否大于等于生产者发送消息后消息服务器返回下一条消息的起始偏移量，
     * 如果是则表示主从同步复制已经完成，华兴消息发送线程，否则等待1s再次判断，每一个任务在一批任务中循环判断5次，
     * 消息发送者返回有两种情况：等待超过5s或者GroupTransferService通知主从复制完成。
     * 可以通过{@link MessageStoreConfig#syncFlushTimeout}来设置发送线程等待超时时间。
     */
    class GroupTransferService extends ServiceThread {

        private final WaitNotifyObject notifyTransferObject = new WaitNotifyObject();
        private volatile List<CommitLog.GroupCommitRequest> requestsWrite = new ArrayList<>();
        private volatile List<CommitLog.GroupCommitRequest> requestsRead = new ArrayList<>();

        /**
         * GroupTransferService线程每次处理的request容器
         * 这是一个设计亮点，避免了任务提交与任务执行的锁冲突。
         *
         * 在提交刷盘任务时将任务提交至requestsWrite中
         * 而当前线程中的执行的刷盘任务是requestsWrite中的，执行完后将requestsWrite清除，
         * 再交换requestsWrite与requestsRead中的值
         */
        public synchronized void putRequest(final CommitLog.GroupCommitRequest request) {
            synchronized (this.requestsWrite) {
                this.requestsWrite.add(request);
            }
            if (hasNotified.compareAndSet(false, true)) {
                waitPoint.countDown(); // notify
            }
        }

        public void notifyTransferSome() {
            this.notifyTransferObject.wakeup();
        }

        private void swapRequests() {
            List<CommitLog.GroupCommitRequest> tmp = this.requestsWrite;
            this.requestsWrite = this.requestsRead;
            this.requestsRead = tmp;
        }

        /**
         * 该方法在Master收到从服务器的拉取请求后被调用，标识从服务器当前已同步的偏移量，
         * 既然已经收到从服务器的反馈信息，需要唤醒某些消息发送线程。如果从服务器收到的确认偏移量大于push2SlaveMaxOffset
         * 则更新push2SlaveMaxOffset，然后唤醒GroupTransferService线程，个消息发送者线程再次判断自己本次发送的消息是否已经
         * 成功复制到从服务器。
         */
        private void doWaitTransfer() {
            synchronized (this.requestsRead) {
                if (!this.requestsRead.isEmpty()) {
                    for (CommitLog.GroupCommitRequest req : this.requestsRead) {
                        //如果已经同步到从服务器的最大偏移量 > 当前消息的偏移量
                        boolean transferOK = HAService.this.push2SlaveMaxOffset.get() >= req.getNextOffset();
                        //循环5次
                        for (int i = 0; !transferOK && i < 5; i++) {
                            //还未同步到从服务器则等待1s后再次判断
                            this.notifyTransferObject.waitForRunning(1000);
                            transferOK = HAService.this.push2SlaveMaxOffset.get() >= req.getNextOffset();
                        }

                        if (!transferOK) {
                            log.warn("transfer messsage to slave timeout, " + req.getNextOffset());
                        }

                        //通知当前消息的偏移量的消息的主从复制结果
                        req.wakeupCustomer(transferOK);
                    }

                    this.requestsRead.clear();
                }
            }
        }

        public void run() {
            log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    //每10ms执行一次
                    this.waitForRunning(10);
                    this.doWaitTransfer();
                } catch (Exception e) {
                    log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            log.info(this.getServiceName() + " service end");
        }

        @Override
        protected void onWaitEnd() {
            this.swapRequests();
        }

        @Override
        public String getServiceName() {
            return GroupTransferService.class.getSimpleName();
        }
    }

    /**
     * 主从同步client端实现类
     */
    class HAClient extends ServiceThread {
        /**
         * socket读缓冲区大小
         */
        private static final int READ_MAX_BUFFER_SIZE = 1024 * 1024 * 4;
        /**
         * master地址
         * @see org.apache.rocketmq.broker.BrokerController#initialize()
         * @see org.apache.rocketmq.broker.BrokerController#doRegisterBrokerAll(boolean, boolean, org.apache.rocketmq.common.protocol.body.TopicConfigSerializeWrapper)
         * @see DefaultMessageStore#updateHaMasterAddress(java.lang.String)
         * 当broker启动时当broker为slave时将设置masterAddress为更新配置文件中的haMasterAddress属性值
         * 如果broeker为slave，但是配置masterAddress为空，并不会报错，只是不会执行主从复制。
         *
         */
        private final AtomicReference<String> masterAddress = new AtomicReference<>();
        /**
         * slave向master发起主从同步的拉取偏移量
         * @see HAClient#reportSlaveMaxOffset(long)
         */
        private final ByteBuffer reportOffset = ByteBuffer.allocate(8);
        /**
         * 网络传输通道
         */
        private SocketChannel socketChannel;
        /**
         * NIO时间选择器
         */
        private Selector selector;
        /**
         * 上一次写入时间戳
         */
        private long lastWriteTimestamp = System.currentTimeMillis();

        /**
         * 反馈slave当前的复制进度，commitlog文件最大偏移量
         */
        private long currentReportedOffset = 0;
        /**
         * 本次已读取的读缓冲(byteBufferRead)的指针
         */
        private int dispatchPostion = 0;
        /**
         * 读缓冲区大小，大小为4M
         * @see HAClient#processReadEvent()
         * 源码中 byteBufferRead一直时处于写模式的
         * 接position是写指针，limit是缓冲区大小
         *
         * 读取的话需要通过dispatchPostion读取
         */
        private ByteBuffer byteBufferRead = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);
        /**
         * 读缓冲区备份，与byteBufferRead进行交换。
         */
        private ByteBuffer byteBufferBackup = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);

        public HAClient() throws IOException {
            this.selector = RemotingUtil.openSelector();
        }

        public void updateMasterAddress(final String newAddr) {
            String currentAddr = this.masterAddress.get();
            if (currentAddr == null || !currentAddr.equals(newAddr)) {
                this.masterAddress.set(newAddr);
                log.info("update master address, OLD: " + currentAddr + " NEW: " + newAddr);
            }
        }

        private boolean isTimeToReportOffset() {
            long interval =
                HAService.this.defaultMessageStore.getSystemClock().now() - this.lastWriteTimestamp;
            boolean needHeart = interval > HAService.this.defaultMessageStore.getMessageStoreConfig()
                .getHaSendHeartbeatInterval();

            return needHeart;
        }

        /**
         * 向master反馈拉取偏移量，有两层意义
         * 1、对于slave来说，是发送下次待拉取的消息偏移量，
         * 2、对于master来说，即可以认为是本次拉取的消息偏移量，也可以理解为salve的消息同步的ack确认消息。
         */
        private boolean reportSlaveMaxOffset(final long maxOffset) {
            this.reportOffset.position(0);
            this.reportOffset.limit(8);
            this.reportOffset.putLong(maxOffset);
            //写模式切换到读模式 可以通过 reportOffset.flip();代替
            this.reportOffset.position(0);
            this.reportOffset.limit(8);

            //调用网络通道的write反法是在一个循环中反复判断reportOffset是否全部写入到通道中，
            //这是由于nio是一个非阻塞io，调用一次write方法不一定会将reportOffset可读字节全部写入。
            for (int i = 0; i < 3 && this.reportOffset.hasRemaining(); i++) {
                try {
                    this.socketChannel.write(this.reportOffset);
                } catch (IOException e) {
                    log.error(this.getServiceName()
                        + "reportSlaveMaxOffset this.socketChannel.write exception", e);
                    return false;
                }
            }

            return !this.reportOffset.hasRemaining();
        }

        private void reallocateByteBuffer() {
            //获取还未读取的在缓冲区的数据带下
            int remain = READ_MAX_BUFFER_SIZE - this.dispatchPostion;
            if (remain > 0) {
                //将byteBufferRead切换为读模式
                this.byteBufferRead.position(this.dispatchPostion);

                //byteBufferRead中未读取的数据拷贝至byteBufferBackup中
                this.byteBufferBackup.position(0);
                this.byteBufferBackup.limit(READ_MAX_BUFFER_SIZE);
                this.byteBufferBackup.put(this.byteBufferRead);
            }

            //交换byteBufferBackup与byteBufferRead
            this.swapByteBuffer();

            //再次将byteBufferRead切换成写模式
            this.byteBufferRead.position(remain);
            this.byteBufferRead.limit(READ_MAX_BUFFER_SIZE);
            //然后新的byteBufferRead中的读指针为0，因为里面的数据都是未处理的。
            this.dispatchPostion = 0;
        }

        private void swapByteBuffer() {
            ByteBuffer tmp = this.byteBufferRead;
            this.byteBufferRead = this.byteBufferBackup;
            this.byteBufferBackup = tmp;
        }

        /**
         * 处理网络读请求，即处理从master服务器传回的消息数据。
         */
        private boolean processReadEvent() {
            int readSizeZeroTimes = 0;
            //循环判断byteBufferRead中是否有剩余空间可写。
            while (this.byteBufferRead.hasRemaining()) {
                try {
                    //将通道中的数据写入缓冲区byteBufferRead中
                    int readSize = this.socketChannel.read(this.byteBufferRead);
                    if (readSize > 0) {
                        //读取到的字节数大于0，更新最后一次写入时间戳
                        lastWriteTimestamp = HAService.this.defaultMessageStore.getSystemClock().now();
                        //重置读取到0字节的次数为0
                        readSizeZeroTimes = 0;
                        //将读取到的所有信息全部追加到内存映射文件中，然后再次反馈消息拉取进度给服务器。
                        boolean result = this.dispatchReadRequest();
                        if (!result) {
                            log.error("HAClient, dispatchReadRequest error");
                            return false;
                        }
                    } else if (readSize == 0) {
                        //如果连续3次读取到0字节，则结束本次读，返回true。
                        if (++readSizeZeroTimes >= 3) {
                            break;
                        }
                    } else {
                        //读取到的字节数小于0，返回false
                        log.info("HAClient, processReadEvent read socket < 0");
                        return false;
                    }
                } catch (IOException e) {
                    //异常返回false
                    log.info("HAClient, processReadEvent read socket exception", e);
                    return false;
                }
            }

            return true;
        }

        /**
         * 将读取到的所有信息全部追加到内存映射文件中，然后再次反馈消息拉取进度给服务器。
         */
        private boolean dispatchReadRequest() {
            /**
             * 前12个字节为消息头。master推送的消息物理偏移量量 + 消息大小
             * @see HAConnection.WriteSocketService#headerSize
             */
            final int msgHeaderSize = 8 + 4; // phyoffset + size
            //获取byteBufferRead的写指针
            int readSocketPos = this.byteBufferRead.position();

            while (true) {
                //diff为缓冲区中未处理的字节数量（即可读取的字节数），最大写指针-读取指针
                int diff = this.byteBufferRead.position() - this.dispatchPostion;
                if (diff >= msgHeaderSize) {
                    //获取master推送的消息物理偏移量
                    long masterPhyOffset = this.byteBufferRead.getLong(this.dispatchPostion);
                    //获取消息大小
                    int bodySize = this.byteBufferRead.getInt(this.dispatchPostion + 8);

                    //获取slave commotlog中的最大消息物理偏移量
                    long slavePhyOffset = HAService.this.defaultMessageStore.getMaxPhyOffset();

                    //master推送的消息物理偏移量必须等于当前slave的最大物理偏移量
                    if (slavePhyOffset != 0) {
                        if (slavePhyOffset != masterPhyOffset) {
                            log.error("master pushed offset not equal the max phy offset in slave, SLAVE: "
                                + slavePhyOffset + " MASTER: " + masterPhyOffset);
                            return false;
                        }
                    }

                    //缓冲区可读取的字节数 >= 消息头+消息体大小
                    if (diff >= (msgHeaderSize + bodySize)) {
                        byte[] bodyData = new byte[bodySize];
                        //将byteBufferRead切换成读模式
                        this.byteBufferRead.position(this.dispatchPostion + msgHeaderSize);
                        //读取消息数据
                        this.byteBufferRead.get(bodyData);

                        //将消息追加到commitlog文件中
                        HAService.this.defaultMessageStore.appendToCommitLog(masterPhyOffset, bodyData);

                        //又将byteBufferRead还原成写模式
                        this.byteBufferRead.position(readSocketPos);
                        //更新已读取的缓冲区的写指针
                        this.dispatchPostion += msgHeaderSize + bodySize;

                        //再次反馈消息拉取进度给服务器
                        if (!reportSlaveMaxOffsetPlus()) {
                            return false;
                        }

                        continue;
                    }
                }

                //如果缓冲区中没有空间可写了
                if (!this.byteBufferRead.hasRemaining()) {
                    this.reallocateByteBuffer();
                }

                break;
            }

            return true;
        }

        private boolean reportSlaveMaxOffsetPlus() {
            boolean result = true;
            //获取最大偏移量
            long currentPhyOffset = HAService.this.defaultMessageStore.getMaxPhyOffset();
            if (currentPhyOffset > this.currentReportedOffset) {
                //大于缓存的偏移量则更新，且想master反馈偏移量。
                this.currentReportedOffset = currentPhyOffset;
                result = this.reportSlaveMaxOffset(this.currentReportedOffset);
                if (!result) {
                    this.closeMaster();
                    log.error("HAClient, reportSlaveMaxOffset error, " + this.currentReportedOffset);
                }
            }

            return result;
        }

        /**
         * slave连接master。
         */
        private boolean connectMaster() throws ClosedChannelException {
            //如果socketChannel为空，则尝试连接master
            if (null == socketChannel) {
                String addr = this.masterAddress.get();
                if (addr != null) {
                    //如果master地址不为空，则建立到master的tcp连接
                    SocketAddress socketAddress = RemotingUtil.string2SocketAddress(addr);
                    if (socketAddress != null) {
                        this.socketChannel = RemotingUtil.connect(socketAddress);
                        if (this.socketChannel != null) {
                            //注册OP_READ事件（一个有数据可读的通道可以说是“读就绪”）
                            this.socketChannel.register(this.selector, SelectionKey.OP_READ);
                        }
                    }
                }

                //初始化currentReportedOffset为commitlog文件的最大偏移量
                this.currentReportedOffset = HAService.this.defaultMessageStore.getMaxPhyOffset();

                //lastWriteTimestamp为当前时间
                this.lastWriteTimestamp = System.currentTimeMillis();
            }

            return this.socketChannel != null;
        }

        private void closeMaster() {
            if (null != this.socketChannel) {
                try {

                    SelectionKey sk = this.socketChannel.keyFor(this.selector);
                    if (sk != null) {
                        sk.cancel();
                    }

                    this.socketChannel.close();

                    this.socketChannel = null;
                } catch (IOException e) {
                    log.warn("closeMaster exception. ", e);
                }

                this.lastWriteTimestamp = 0;
                this.dispatchPostion = 0;

                this.byteBufferBackup.position(0);
                this.byteBufferBackup.limit(READ_MAX_BUFFER_SIZE);

                this.byteBufferRead.position(0);
                this.byteBufferRead.limit(READ_MAX_BUFFER_SIZE);
            }
        }

        @Override
        public void run() {
            log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    //slave连接master。
                    if (this.connectMaster()) {

                        //成功连接到master broker

                        //判断是否需要向master反馈当前拉取偏移量，master与slave的HA心跳间隔默认5s
                        if (this.isTimeToReportOffset()) {
                            //向master反馈拉取偏移量
                            boolean result = this.reportSlaveMaxOffset(this.currentReportedOffset);
                            if (!result) {
                                this.closeMaster();
                            }
                        }

                        ///每隔1s处理一次读就绪事件，
                        this.selector.select(1000);

                        //处理网络读请求，即处理从master服务器传回的消息数据。
                        boolean ok = this.processReadEvent();
                        if (!ok) {
                            this.closeMaster();
                        }

                        //再次反馈消息拉取进度给服务器
                        if (!reportSlaveMaxOffsetPlus()) {
                            continue;
                        }

                        long interval =
                            HAService.this.getDefaultMessageStore().getSystemClock().now()
                                - this.lastWriteTimestamp;
                        //如果20s 没有读取到master服务器的发送过来的数据，则需要关闭连接，初始化数据
                        if (interval > HAService.this.getDefaultMessageStore().getMessageStoreConfig()
                            .getHaHousekeepingInterval()) {
                            log.warn("HAClient, housekeeping, found this connection[" + this.masterAddress
                                + "] expired, " + interval);
                            this.closeMaster();
                            log.warn("HAClient, master not response some time, so close connection");
                        }
                    } else {
                        this.waitForRunning(1000 * 5);
                    }
                } catch (Exception e) {
                    log.warn(this.getServiceName() + " service has exception. ", e);
                    this.waitForRunning(1000 * 5);
                }
            }

            log.info(this.getServiceName() + " service end");
        }
        // private void disableWriteFlag() {
        // if (this.socketChannel != null) {
        // SelectionKey sk = this.socketChannel.keyFor(this.selector);
        // if (sk != null) {
        // int ops = sk.interestOps();
        // ops &= ~SelectionKey.OP_WRITE;
        // sk.interestOps(ops);
        // }
        // }
        // }
        // private void enableWriteFlag() {
        // if (this.socketChannel != null) {
        // SelectionKey sk = this.socketChannel.keyFor(this.selector);
        // if (sk != null) {
        // int ops = sk.interestOps();
        // ops |= SelectionKey.OP_WRITE;
        // sk.interestOps(ops);
        // }
        // }
        // }

        @Override
        public String getServiceName() {
            return HAClient.class.getSimpleName();
        }
    }
}

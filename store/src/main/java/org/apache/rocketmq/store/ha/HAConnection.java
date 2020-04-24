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
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.store.SelectMappedBufferResult;

/**
 * HA Master服务端HA连接对象的封装，与Broker从服务器的网络读写实现类
 * Master服务端在收到从服务器的链接请求后，会将主从服务器的SocketChannel封装成HAConnection对象，
 * 实现主从服务器间的读写操作。
 * @see HAService.AcceptSocketService#run()
 *
 */
public class HAConnection {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    /**
     * HAService对象
     */
    private final HAService haService;
    /**
     * 网络通信socket通道
     */
    private final SocketChannel socketChannel;
    /**
     * 客户端连接地址
     */
    private final String clientAddr;
    /**
     * 服务端向从服务器写数据服务类
     */
    private WriteSocketService writeSocketService;
    /**
     * 服务端从从服务器读数据服务类
     */
    private ReadSocketService readSocketService;

    /**
     * 从服务器请求拉取数据的偏移量
     * @see ReadSocketService#processReadEvent()
     * @see WriteSocketService#run()
     */
    private volatile long slaveRequestOffset = -1;
    /**
     * 从服务器反馈以拉取完成的数据偏移量
     */
    private volatile long slaveAckOffset = -1;

    public HAConnection(final HAService haService, final SocketChannel socketChannel) throws IOException {
        this.haService = haService;
        this.socketChannel = socketChannel;
        this.clientAddr = this.socketChannel.socket().getRemoteSocketAddress().toString();
        this.socketChannel.configureBlocking(false);
        this.socketChannel.socket().setSoLinger(false, -1);
        this.socketChannel.socket().setTcpNoDelay(true);
        this.socketChannel.socket().setReceiveBufferSize(1024 * 64);
        this.socketChannel.socket().setSendBufferSize(1024 * 64);
        this.writeSocketService = new WriteSocketService(this.socketChannel);
        this.readSocketService = new ReadSocketService(this.socketChannel);
        this.haService.getConnectionCount().incrementAndGet();
    }

    public void start() {
        this.readSocketService.start();
        this.writeSocketService.start();
    }

    public void shutdown() {
        this.writeSocketService.shutdown(true);
        this.readSocketService.shutdown(true);
        this.close();
    }

    public void close() {
        if (this.socketChannel != null) {
            try {
                this.socketChannel.close();
            } catch (IOException e) {
                HAConnection.log.error("", e);
            }
        }
    }

    public SocketChannel getSocketChannel() {
        return socketChannel;
    }

    /**
     * HA Master网络读实现类
     */
    class ReadSocketService extends ServiceThread {
        /**
         * 网络读取缓冲区大小，默认1M
         */
        private static final int READ_MAX_BUFFER_SIZE = 1024 * 1024;
        /**
         * NIO网络事件选择器
         */
        private final Selector selector;
        /**
         * 网络通道，用于读写的socket通道
         */
        private final SocketChannel socketChannel;
        /**
         * 网络读写缓冲区，默认为1M
         * @see ReadSocketService#processReadEvent()
         * 源码中 byteBufferRead一直时处于写模式的
         * position是写指针，limit是缓冲区大小
         */
        private final ByteBuffer byteBufferRead = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);
        /**
         * byteBufferRead当前读指针
         */
        private int processPostion = 0;
        /**
         * 上次读取数据的时间
         */
        private volatile long lastReadTimestamp = System.currentTimeMillis();

        public ReadSocketService(final SocketChannel socketChannel) throws IOException {
            this.selector = RemotingUtil.openSelector();
            this.socketChannel = socketChannel;
            //注册OP_READ事件（一个有数据可读的通道可以说是“读就绪”）
            this.socketChannel.register(this.selector, SelectionKey.OP_READ);
            this.thread.setDaemon(true);
        }

        @Override
        public void run() {
            HAConnection.log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    //每隔1s处理一次读就绪事件，
                    this.selector.select(1000);
                    //处理网络读请求，即处理slave反馈的消息拉取偏移量。
                    boolean ok = this.processReadEvent();
                    if (!ok) {
                        HAConnection.log.error("processReadEvent error");
                        break;
                    }

                    long interval = HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now() - this.lastReadTimestamp;
                    if (interval > HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig().getHaHousekeepingInterval()) {
                        log.warn("ha housekeeping, found this connection[" + HAConnection.this.clientAddr + "] expired, " + interval);
                        break;
                    }
                } catch (Exception e) {
                    HAConnection.log.error(this.getServiceName() + " service has exception.", e);
                    break;
                }
            }

            this.makeStop();

            writeSocketService.makeStop();

            haService.removeConnection(HAConnection.this);

            HAConnection.this.haService.getConnectionCount().decrementAndGet();

            SelectionKey sk = this.socketChannel.keyFor(this.selector);
            if (sk != null) {
                sk.cancel();
            }

            try {
                this.selector.close();
                this.socketChannel.close();
            } catch (IOException e) {
                HAConnection.log.error("", e);
            }

            HAConnection.log.info(this.getServiceName() + " service end");
        }

        @Override
        public String getServiceName() {
            return ReadSocketService.class.getSimpleName();
        }

        /**
         * 处理网络读请求，即处理slave反馈的消息拉取偏移量。
         */
        private boolean processReadEvent() {
            int readSizeZeroTimes = 0;

            //如果没有剩余的可写空间，说明position==limit==capital
            if (!this.byteBufferRead.hasRemaining()) {
                //那么从头开始处理，这里等同于byteBufferRead.clear()
                this.byteBufferRead.flip();
                //byteBufferRead读指针清零
                this.processPostion = 0;
            }

            //循环判断byteBufferRead中是否有剩余空间可写。
            while (this.byteBufferRead.hasRemaining()) {
                try {
                    //将通道中的数据写入缓冲区byteBufferRead中
                    int readSize = this.socketChannel.read(this.byteBufferRead);
                    if (readSize > 0) {
                        readSizeZeroTimes = 0;
                        //读取到的字节数大于0，更新最后一次写入时间戳
                        this.lastReadTimestamp = HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now();
                        //可读指针大于等于8，表示收到了从服务器的一条拉取消息的请求，
                        if ((this.byteBufferRead.position() - this.processPostion) >= 8) {
                            int pos = this.byteBufferRead.position() - (this.byteBufferRead.position() % 8);
                            /**
                             * 因为从服务器向master服务器反馈拉取偏移量时，偏移量暂用8个字节
                             * @see HAService.HAClient#reportSlaveMaxOffset(long)
                             * 这里为了取拉取偏移量的期食品偏移量为8的整数倍，是为了处理网络中的沾包？
                             *
                             * -8为了获取其实偏移量，通过起始偏移量读取8个字节后去从服务器向master服务器反馈拉取偏移量
                             */
                            long readOffset = this.byteBufferRead.getLong(pos - 8);
                            //设置当前byteBufferRead的读指针。
                            this.processPostion = pos;
                            // 设置从服务器的拉取偏移量
                            HAConnection.this.slaveAckOffset = readOffset;
                            //master第一次收到slave反馈的拉取偏移量，那么更新，正常slaveRequestOffset应该为0，当然如果主从服务器均重启，且从磁盘中恢复了之前的commitlog文件就不为0了
                            if (HAConnection.this.slaveRequestOffset < 0) {
                                HAConnection.this.slaveRequestOffset = readOffset;
                                log.info("slave[" + HAConnection.this.clientAddr + "] request offset " + readOffset);
                            }

                            //由于有新的从服务器反馈拉取偏移量，服务端会通知由于同步等待HA复制结果而阻塞的消息发送者线程。
                            HAConnection.this.haService.notifyTransferSome(HAConnection.this.slaveAckOffset);
                        }
                    } else if (readSize == 0) {
                        //如果连续3次读取到0字节，则结束本次读，返回true。
                        if (++readSizeZeroTimes >= 3) {
                            break;
                        }
                    } else {
                        //读取到的字节数小于0，返回false
                        log.error("read socket[" + HAConnection.this.clientAddr + "] < 0");
                        return false;
                    }
                } catch (IOException e) {
                    //异常返回false
                    log.error("processReadEvent exception", e);
                    return false;
                }
            }

            return true;
        }
    }

    /**
     * HA Master网络写实现类
     */
    class WriteSocketService extends ServiceThread {
        /**
         * NIO网络事件选择器
         */
        private final Selector selector;
        /**
         * 网络通道，用于读写的socket通道
         */
        private final SocketChannel socketChannel;

        /**
         * 消息头长度 消息物理偏移量8字节 + 消息长度4字节
         */
        private final int headerSize = 8 + 4;
        /**
         * 消息头数据
         */
        private final ByteBuffer byteBufferHeader = ByteBuffer.allocate(headerSize);
        /**
         * 下一次传输的物理偏移量
         * @see WriteSocketService#run()
         */
        private long nextTransferFromWhere = -1;
        /**
         * 根据消息偏移量查找消息的结果
         */
        private SelectMappedBufferResult selectMappedBufferResult;
        /**
         * 上一次数据是否传输完毕
         */
        private boolean lastWriteOver = true;
        /**
         * 上次写入的时间戳
         */
        private long lastWriteTimestamp = System.currentTimeMillis();

        public WriteSocketService(final SocketChannel socketChannel) throws IOException {
            this.selector = RemotingUtil.openSelector();
            this.socketChannel = socketChannel;
            //注册OP_WRITE事件（等待写数据的通道可以说是“写就绪”）
            this.socketChannel.register(this.selector, SelectionKey.OP_WRITE);
            this.thread.setDaemon(true);
        }

        @Override
        public void run() {
            HAConnection.log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    //每隔1s处理一次写就绪事件，
                    this.selector.select(1000);

                    //如果slaveRequestOffset等于-1，说明master还未收到从服务器的拉取请求，放弃本次事件处理，
                    if (-1 == HAConnection.this.slaveRequestOffset) {
                        Thread.sleep(10);
                        continue;
                    }

                    //如果nextTransferFromWhere等于-1，表示初次进行数据传输，计算待传输的物理偏移量，
                    if (-1 == this.nextTransferFromWhere) {
                        //如果slaveRequestOffset等于0，正常情况下第一次传输slaveRequestOffset=0
                        //当然如果主从服务器均重启，且从磁盘中恢复了之前的commitlog文件就不为0了
                        if (0 == HAConnection.this.slaveRequestOffset) {
                            //获取commitLog文件最大偏移量开始传输，
                            long masterOffset = HAConnection.this.haService.getDefaultMessageStore().getCommitLog().getMaxOffset();
                            //获取commitLog文件最大偏移量所在的MappedFile的起始偏移量，从此开始传输。
                            masterOffset =
                                masterOffset
                                    - (masterOffset % HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig()
                                    .getMapedFileSizeCommitLog());

                            if (masterOffset < 0) {
                                masterOffset = 0;
                            }

                            this.nextTransferFromWhere = masterOffset;
                        } else {
                            //否则根据从服务器的拉取请求偏移量开始传输。
                            this.nextTransferFromWhere = HAConnection.this.slaveRequestOffset;
                        }

                        log.info("master transfer data from " + this.nextTransferFromWhere + " to slave[" + HAConnection.this.clientAddr
                            + "], and slave request " + HAConnection.this.slaveRequestOffset);
                    }

                    //判断上次写事件是否已经将信息全部写入客户端。
                    if (this.lastWriteOver) {

                        //计算当前时间与上次些时间间隔
                        long interval =
                            HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now() - this.lastWriteTimestamp;

                        //大于心跳检测时间，则发送一个心跳包
                        if (interval > HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig()
                            .getHaSendHeartbeatInterval()) {

                            /**
                             * 心跳包长度12字节，从服务器拉取偏移量 + size，消息长度默认为0，避免长连接处于空闲被关闭。
                             * @see HAService.HAClient#run()
                             * 中20s未更新写入时间则关闭长连接
                             */
                            // Build Header
                            this.byteBufferHeader.position(0);
                            this.byteBufferHeader.limit(headerSize);
                            this.byteBufferHeader.putLong(this.nextTransferFromWhere);
                            this.byteBufferHeader.putInt(0);
                            this.byteBufferHeader.flip();

                            this.lastWriteOver = this.transferData();
                            if (!this.lastWriteOver)
                                continue;
                        }
                    } else {
                        //如果上次数据未写完则先传输上一次数据，如果消息还未全部传输完成，则结束此次时间处理。
                        this.lastWriteOver = this.transferData();
                        if (!this.lastWriteOver)
                            continue;
                    }

                    //根据消息传输偏移量，查找该消息之后的所有的可读消息
                    SelectMappedBufferResult selectResult =
                        HAConnection.this.haService.getDefaultMessageStore().getCommitLogData(this.nextTransferFromWhere);
                    if (selectResult != null) {
                        //找到消息，且查找的消息长度大于配置的HA传输一次同步任务最大传输的字节数。默认32kb
                        int size = selectResult.getSize();
                        if (size > HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig().getHaTransferBatchSize()) {
                            //设置传输字节数为32kb
                            size = HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig().getHaTransferBatchSize();
                        }

                        long thisOffset = this.nextTransferFromWhere;
                        //设置下次传输的物理偏移量
                        this.nextTransferFromWhere += size;

                        //通过limit设置传输指定字节的长度。
                        selectResult.getByteBuffer().limit(size);
                        this.selectMappedBufferResult = selectResult;

                        //设置消息头
                        // Build Header
                        this.byteBufferHeader.position(0);
                        this.byteBufferHeader.limit(headerSize);
                        this.byteBufferHeader.putLong(thisOffset);
                        this.byteBufferHeader.putInt(size);
                        this.byteBufferHeader.flip();

                        this.lastWriteOver = this.transferData();
                    } else {

                        //如果未找到消息，则通知所有等待线程急继续等待100ms
                        HAConnection.this.haService.getWaitNotifyObject().allWaitForRunning(100);
                    }
                } catch (Exception e) {

                    HAConnection.log.error(this.getServiceName() + " service has exception.", e);
                    break;
                }
            }

            if (this.selectMappedBufferResult != null) {
                this.selectMappedBufferResult.release();
            }

            this.makeStop();

            readSocketService.makeStop();

            haService.removeConnection(HAConnection.this);

            SelectionKey sk = this.socketChannel.keyFor(this.selector);
            if (sk != null) {
                sk.cancel();
            }

            try {
                this.selector.close();
                this.socketChannel.close();
            } catch (IOException e) {
                HAConnection.log.error("", e);
            }

            HAConnection.log.info(this.getServiceName() + " service end");
        }

        private boolean transferData() throws Exception {
            int writeSizeZeroTimes = 0;
            // Write Header
            // 循环判断byteBufferHeader中是否有剩余空间可写。
            while (this.byteBufferHeader.hasRemaining()) {
                //那么将数据写入同道中
                int writeSize = this.socketChannel.write(this.byteBufferHeader);
                if (writeSize > 0) {
                    //写入数据大于0，那么更新lastWriteTimestamp
                    writeSizeZeroTimes = 0;
                    this.lastWriteTimestamp = HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now();
                } else if (writeSize == 0) {
                    //如果连续3次写入0字节，则结束本次写
                    if (++writeSizeZeroTimes >= 3) {
                        break;
                    }
                } else {
                    //异常
                    throw new Exception("ha master write header error < 0");
                }
            }

            //如果selectMappedBufferResult为空，说明byteBufferHeader重的消息大小为0字节，是为了维持与slave的心跳包。
            if (null == this.selectMappedBufferResult) {
                //通过判断byteBufferHeader中的数据是否全部写入判断此次数据是否传输完毕
                return !this.byteBufferHeader.hasRemaining();
            }

            writeSizeZeroTimes = 0;

            // Write Body
            // 消息头传输完成开始传输消息体
            if (!this.byteBufferHeader.hasRemaining()) {
                // 循环判断byteBuffer中是否有剩余空间可写。
                while (this.selectMappedBufferResult.getByteBuffer().hasRemaining()) {
                    int writeSize = this.socketChannel.write(this.selectMappedBufferResult.getByteBuffer());
                    if (writeSize > 0) {
                        //写入数据大于0，那么更新lastWriteTimestamp
                        writeSizeZeroTimes = 0;
                        this.lastWriteTimestamp = HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now();
                    } else if (writeSize == 0) {
                        //如果连续3次写入0字节，则结束本次写
                        if (++writeSizeZeroTimes >= 3) {
                            break;
                        }
                    } else {
                        throw new Exception("ha master write body error < 0");
                    }
                }
            }

            //通过消息头与消息体中的数据是否均全部写入 来判断此次数据是否传输完毕
            boolean result = !this.byteBufferHeader.hasRemaining() && !this.selectMappedBufferResult.getByteBuffer().hasRemaining();

            if (!this.selectMappedBufferResult.getByteBuffer().hasRemaining()) {
                this.selectMappedBufferResult.release();
                this.selectMappedBufferResult = null;
            }

            return result;
        }

        @Override
        public String getServiceName() {
            return WriteSocketService.class.getSimpleName();
        }

        @Override
        public void shutdown() {
            super.shutdown();
        }
    }
}

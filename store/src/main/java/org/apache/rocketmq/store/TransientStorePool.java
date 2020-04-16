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
package org.apache.rocketmq.store;

import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import java.nio.ByteBuffer;
import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.util.LibC;
import sun.nio.ch.DirectBuffer;

/**
 * 当{@link org.apache.rocketmq.store.config.MessageStoreConfig#transientStorePoolEnable}为true时
 * 且是异步刷盘且是master broker
 * @see org.apache.rocketmq.store.config.MessageStoreConfig#isTransientStorePoolEnable()
 * 启用“读写”分离，消息发送时消息先追加到DirectByteBuffer(堆外内存)中，然后在异步刷盘机制下，
 * 会将DirectByteBuffer中的内容提交到PageCache，然后刷写到磁盘。消息拉取时，直接从PageCache中拉取，实现了读写分离，减轻了PageCaceh的压力。
 *
 * RocketMQ会申请一个与目标物理文件（commitLog）同样大小的堆外内存，该堆外内存将使用内存锁定，确保不会被置换到虚拟内存中去，消息首先追加到堆外内存，
 * 然后提交到与物理文件的内存中，再flush到磁盘。
 *
 * 方案缺点：
 * 会增加数据丢失的可能性，如果Broker JVM进程异常退出，提交到PageCache中的消息是不会丢失的，
 * 但存在堆外内存(DirectByteBuffer)中但还未提交到PageCache中的这部分消息，将会丢失。但通常情况下，RocketMQ进程退出的可能性不大。
 */
/**
 * 在消息写入消息时，首先从池子中获取一个DirectByteBuffer进行消息的追加。当5个DirectByteBuffer全部写满消息后，该如何处理呢？
 * 从RocketMQ的设计中来看，同一时间，只会对一个commitlog文件进行顺序写，写完一个后，继续创建一个新的commitlog文件。
 * 故TransientStorePool的设计思想是循环利用这5个DirectByteBuffer，
 * 只需要写入到某个DirectByteBuffer的内容全部被提交到PageCache后，即可重复利用{@link MappedFile#commit(int)}。
 * 因此不会发生内存溢出。
 */
public class TransientStorePool {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    /**
     * 初始化DirectByteBuffer堆外内存个数
     */
    private final int poolSize;
    /**
     * 每个堆外内存大小
     */
    private final int fileSize;
    private final Deque<ByteBuffer> availableBuffers;
    private final MessageStoreConfig storeConfig;

    /**

     * @param storeConfig
     */
    public TransientStorePool(final MessageStoreConfig storeConfig) {
        this.storeConfig = storeConfig;
        this.poolSize = storeConfig.getTransientStorePoolSize();
        this.fileSize = storeConfig.getMapedFileSizeCommitLog();
        this.availableBuffers = new ConcurrentLinkedDeque<>();
    }

    /**
     * It's a heavy init method.
     * TransientStorePool默认会初始化5个DirectByteBuffer(堆外内存)，并提供内存锁定功能，即这部分内存不会被置换，可以通过transientStorePoolSize参数控制。
     */
    public void init() {
        for (int i = 0; i < poolSize; i++) {
            ByteBuffer byteBuffer = ByteBuffer.allocateDirect(fileSize);

            final long address = ((DirectBuffer) byteBuffer).address();
            Pointer pointer = new Pointer(address);
            //初始化时使用mlock将内存锁定，防止pagecache被os交换到swap区域。
            LibC.INSTANCE.mlock(pointer, new NativeLong(fileSize));

            availableBuffers.offer(byteBuffer);
        }
    }

    /**
     * //销毁内存池
     */
    public void destroy() {
        //取消对内存的锁定
        for (ByteBuffer byteBuffer : availableBuffers) {
            final long address = ((DirectBuffer) byteBuffer).address();
            Pointer pointer = new Pointer(address);
            LibC.INSTANCE.munlock(pointer, new NativeLong(fileSize));
        }
    }

    /**
     * 重用byteBuffer
     */
    public void returnBuffer(ByteBuffer byteBuffer) {
        byteBuffer.position(0);
        byteBuffer.limit(fileSize);
        this.availableBuffers.offerFirst(byteBuffer);
    }

    /**
     * 从池中获取ByteBuffer
     */
    public ByteBuffer borrowBuffer() {
        //非阻塞弹出队头元素，如果没有启用暂存池，则
        //不会调用init方法，队列中就没有元素，这里返回null
        //其次，如果队列中所有元素都被借用出去，队列也为空
        //此时也会返回null
        ByteBuffer buffer = availableBuffers.pollFirst();
        if (availableBuffers.size() < poolSize * 0.4) {
            log.warn("TransientStorePool only remain {} sheets.", availableBuffers.size());
        }
        return buffer;
    }

    /**
     * 剩下可借出的ByteBuffer数量
     */
    public int remainBufferNumbs() {
        //如果启用了暂存池，则返回队列中元素个数
        if (storeConfig.isTransientStorePoolEnable()) {
            return availableBuffers.size();
        }
        //否则返会Integer.MAX_VALUE
        return Integer.MAX_VALUE;
    }
}

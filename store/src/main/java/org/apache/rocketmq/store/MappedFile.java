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
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.util.LibC;
import sun.nio.ch.DirectBuffer;

/**
 * mmap是一种将文件映射到虚拟内存的技术，可以将文件在磁盘位置的地址和在虚拟内存中的虚拟地址通过映射对应起来，
 * 之后就可以在内存这块区域进行读写数据，而不必调用系统级别的read,wirte这些函数，从而提升IO操作性能，
 * 另外一点就是mmap后的虚拟内存大小必须是内存页大小(通常是4K)的倍数，之所以这么做是为了匹配内存操作。
 *
 * PageCache是OS对文件的缓存，用于加速对文件的读写。一般来说，程序对文件进行顺序读写的速度几乎接近于内存的读写访问，
 * 这里的主要原因就是在于OS使用PageCache机制对读写访问操作进行了性能优化，将一部分的内存用作PageCache。
 * 1. 对于数据文件的读取，如果一次读取文件时出现未命中PageCache的情况，OS从物理磁盘上访问读取文件的同时，
 * 会顺序对其他相邻块的数据文件进行预读取（ps：顺序读入紧随其后的少数几个页面）。这样，只要下次访问的文件已经被加载至PageCache时，读取操作的速度基本等于访问内存。
 * 2. 对于数据文件的写入，OS会先写入至Cache内，随后通过异步的方式由pdflush内核线程将Cache内的数据刷盘至物理磁盘上。
 * 对于文件的顺序读写操作来说，读和写的区域都在OS的PageCache内，此时读写性能接近于内存。RocketMQ的大致做法是，
 * 将数据文件映射到OS的虚拟内存中（通过JDK NIO的MappedByteBuffer），写消息的时候首先写入PageCache，
 * 并通过异步刷盘的方式将消息批量的做持久化（同时也支持同步刷盘）；订阅消费消息时（对CommitLog操作是随机读取），
 * 由于PageCache的局部性热点原理且整体情况下还是从旧到新的有序读，因此大部分情况下消息还是可以直接从Page Cache中读取，不会产生太多的缺页（Page Fault）中断而从磁盘读取。
 *
 * 由于使用了内存映射文件，只要存在于存储目录下的文件都需要对应创建内存映射文件，如果不定时将已消费的消息从存储文件中删除，
 * 会造成极大的内存压力与资源浪费，所以RocketMQ采取定时删除存储文件的策略。
 * 会调用{@link org.apache.rocketmq.store.MappedFile#destroy(long)}
 *
 *
 */
/**
 *
 * 消息读取时{@link org.apache.rocketmq.store.MappedFile#selectMappedBuffer(int, int)}，是从mappedByteBuffer中读(pageCache)。
 * 而写入消息时{@link MappedFile#appendMessagesInner(org.apache.rocketmq.common.message.MessageExt, org.apache.rocketmq.store.AppendMessageCallback)}
 * 方法中如果开启了transientStorePoolEnable机制那么写入的是writeBuffer
 *
 * 所以如果开启transientStorePoolEnable机制，是不是有了读写分离的效果，先写入writerBuffer中，读却是从mappedByteBuffer中读取。
 *
 * 通常有如下两种方式进行读写：
 *
 * 1. Mmap+PageCache的方式，读写消息都走的是pageCache，这样子读写都在PageCache里面不可避免会有锁的问题，
 *    在并发的读写操作情况下，会出现缺页中断降低，内存加锁，污染页的回写。
 * 2. DirectByteBuffer(堆外内存)+PageCache的两层架构方式，这样子可以实现读写消息分离，写入消息时候写到的是DirectByteBuffer——堆外内存中,
 *    读消息走的是PageCache(对于,DirectByteBuffer是两步刷盘，一步是刷到PageCache，还有一步是刷到磁盘文件中)，带来的好处就是，
 *    避免了内存操作的很多容易堵的地方，降低了时延，比如说缺页中断降低，内存加锁，污染页的回写。
 */
/**
 * 在MappedFile的设计中，只有提交了的数据（写入到{@link MappedFile#mappedByteBuffer}或{@link MappedFile#fileChannel}）才是安全的数据
 * 刷盘时是{@link MappedFile#mappedByteBuffer} -> 磁盘
 * 或者
 * {@link MappedFile#writeBuffer} -> {@link MappedFile#fileChannel}） -> 磁盘
 *
 * 在往Buffer中写入数据的时候不管是{@link MappedFile#mappedByteBuffer}还是{@link MappedFile#writeBuffer}
 * 这两个变量中的position、limit都不会变。position会一直是0，而limit一直是fileSize大小。
 * 但是在往里面写入数据后，有一个指针{@link MappedFile#wrotePosition}记录当前写位置。
 * 那么就知道当前Buffer中有多少数据。
 * 而对于使用{@link MappedFile#writeBuffer}时，又有{@link MappedFile#committedPosition}记录当前提交的数据。
 *
 */
public class MappedFile extends ReferenceResource {

    /**
     * 操作系统每页大小，默认4K
     * Page Cache 叫做页缓存，而每一页的大小通常是4K，在Linux系统中写入数据的时候并不会直接写到硬盘上，
     * 而是会先写到Page Cache中，并打上dirty标识，由内核线程flusher定期将被打上dirty的页发送给IO调度层，
     * 最后由IO调度决定何时落地到磁盘中，而Linux一般会把还没有使用的内存全拿来给Page Cache使用。
     * 而读的过程也是类似，会先到Page Cache中寻找是否有数据，有的话直接返回，如果没有才会到磁盘中去读取并写入Page Cache然后再次读取Page Cache并返回。
     * 而且读的这个过程中操作系统也会有一个预读的操作，你的每一次读取操作系统都会帮你预读出后面一部分数据，
     * 而且当你一直在使用预读数据的时候，系统会帮你预读出更多的数据(最大到128K)。
     */
    public static final int OS_PAGE_SIZE = 1024 * 4;
    protected static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    /**
     * 当前JVM实例中MappedFile虚拟内存大小
     */
    private static final AtomicLong TOTAL_MAPPED_VIRTUAL_MEMORY = new AtomicLong(0);

    /**
     * 当前所有的MappedFile文件数量
     */
    private static final AtomicInteger TOTAL_MAPPED_FILES = new AtomicInteger(0);

    /**
     * 记录写入commitLog文件中的消息的字节数
     * ByteBuffer写状态下中的position的值
     *
     * 当前该文件的写指针，从0开始（内存映射文件的写指针）
     */
    protected final AtomicInteger wrotePosition = new AtomicInteger(0);
    //ADD BY ChenYang
    /**
     * 当前文件的提交指针，如果开启{@link MessageStoreConfig#transientStorePoolEnable}
     * 则数据会存储在{@link TransientStorePool#availableBuffers}中即{@link MappedFile#writeBuffer}，
     * 再提交到{@link MappedFile#fileChannel}再刷写到磁盘。
     * 因为committedPosition指针为提交到{@link MappedFile#fileChannel}中的指针。
     */
    protected final AtomicInteger committedPosition = new AtomicInteger(0);


    /**
     * 刷写到磁盘指针，该指针之前的数据持久化到磁盘
     * 1. {@link MappedFile#writeBuffer}不为空，数据时从{@link MappedFile#writeBuffer}进入到{@link MappedFile#fileChannel}
     * flushedPosition应该等于writeBuffer的{@link MappedFile#committedPosition}，标识这些数据都已经提交到fileChannel中，并且刷盘时会全部刷盘
     * 2. writeBuffer为空，数据是直接进入到mappedByteBuffer，且从mappedByteBuffer中直接刷盘，
     * flushedPosition应该等于mappedByteBuffer的写指针wrotePosition，标识写指针之前的数据都已经刷盘了。
     *
     * 所以更新flushedPosition的值为对应的指针。
     */
    private final AtomicInteger flushedPosition = new AtomicInteger(0);

    /**
     * 文件大小
     * 值为{@link org.apache.rocketmq.store.MappedFileQueue#mappedFileSize}
     */
    protected int fileSize;

    /**
     * 文件通道
     */
    protected FileChannel fileChannel;
    /**
     * Message will put to here first, and then reput to FileChannel if writeBuffer is not null.
     * 堆外内存ByteBuffer，如果不为空，数据首先将存储在该Buffer中，
     * 然后提交到{@link MappedFile#fileChannel}中，并更新提交指针{@link MappedFile#committedPosition}。
     * {@link MessageStoreConfig#transientStorePoolEnable}为true时不为空。
     */
    protected ByteBuffer writeBuffer = null;

    /**
     * 对外内存池，该内存中的内存会提供锁定机制。
     * {@link MessageStoreConfig#transientStorePoolEnable}为true时启用
     */
    protected TransientStorePool transientStorePool = null;

    /**
     * 文件名字
     */
    private String fileName;
    /**
     * 该文件中第一条消息的物理偏移量
     */
    private long fileFromOffset;

    /**
     * 物理文件
     */
    private File file;

    /**
     * 物理文件对应的内存映射Buffer
     */
    private MappedByteBuffer mappedByteBuffer;

    /**
     * 文件最后一次内容写入时间
     */
    private volatile long storeTimestamp = 0;

    /**
     * 标识本文件是不是第一个创建的commitLog文件
     * 即是不是{@link MappedFileQueue#mappedFiles}中的第一个文件
     */
    private boolean firstCreateInQueue = false;

    public MappedFile() {
    }

    public MappedFile(final String fileName, final int fileSize) throws IOException {
        init(fileName, fileSize);
    }

    public MappedFile(final String fileName, final int fileSize,
        final TransientStorePool transientStorePool) throws IOException {
        init(fileName, fileSize, transientStorePool);
    }

    public static void ensureDirOK(final String dirName) {
        if (dirName != null) {
            File f = new File(dirName);
            if (!f.exists()) {
                boolean result = f.mkdirs();
                log.info(dirName + " mkdir " + (result ? "OK" : "Failed"));
            }
        }
    }

    public static void clean(final ByteBuffer buffer) {
        if (buffer == null || !buffer.isDirect() || buffer.capacity() == 0)
            return;
        invoke(invoke(viewed(buffer), "cleaner"), "clean");
    }

    private static Object invoke(final Object target, final String methodName, final Class<?>... args) {
        return AccessController.doPrivileged(new PrivilegedAction<Object>() {
            public Object run() {
                try {
                    Method method = method(target, methodName, args);
                    method.setAccessible(true);
                    return method.invoke(target);
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
        });
    }

    private static Method method(Object target, String methodName, Class<?>[] args)
        throws NoSuchMethodException {
        try {
            return target.getClass().getMethod(methodName, args);
        } catch (NoSuchMethodException e) {
            return target.getClass().getDeclaredMethod(methodName, args);
        }
    }

    private static ByteBuffer viewed(ByteBuffer buffer) {
        String methodName = "viewedBuffer";

        Method[] methods = buffer.getClass().getMethods();
        for (int i = 0; i < methods.length; i++) {
            if (methods[i].getName().equals("attachment")) {
                methodName = "attachment";
                break;
            }
        }

        ByteBuffer viewedBuffer = (ByteBuffer) invoke(buffer, methodName);
        if (viewedBuffer == null)
            return buffer;
        else
            return viewed(viewedBuffer);
    }

    public static int getTotalMappedFiles() {
        return TOTAL_MAPPED_FILES.get();
    }

    public static long getTotalMappedVirtualMemory() {
        return TOTAL_MAPPED_VIRTUAL_MEMORY.get();
    }

    public void init(final String fileName, final int fileSize,
        final TransientStorePool transientStorePool) throws IOException {
        init(fileName, fileSize);
        //如果开启了transientStorePoolEnable,则使用ByteBuffer.allocateDirect(fileSize),创建(java.nio的内存映射机制)。如果未开启，则为空。
        this.writeBuffer = transientStorePool.borrowBuffer();
        this.transientStorePool = transientStorePool;
    }

    private void init(final String fileName, final int fileSize) throws IOException {
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.file = new File(fileName);
        //初始化fileFromOffset为文件名。也就是文件名代表该文件的起始偏移量。
        this.fileFromOffset = Long.parseLong(this.file.getName());
        boolean ok = false;

        ensureDirOK(this.file.getParent());

        try {
            //通过RandomAccessFile创建读写文件通道
            this.fileChannel = new RandomAccessFile(this.file, "rw").getChannel();
            //将文件内容使用NIO的内存映射Buffer将文件映射到内存中
            this.mappedByteBuffer = this.fileChannel.map(MapMode.READ_WRITE, 0, fileSize);
            TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(fileSize);
            TOTAL_MAPPED_FILES.incrementAndGet();
            ok = true;
        } catch (FileNotFoundException e) {
            log.error("create file channel " + this.fileName + " Failed. ", e);
            throw e;
        } catch (IOException e) {
            log.error("map file " + this.fileName + " Failed. ", e);
            throw e;
        } finally {
            if (!ok && this.fileChannel != null) {
                this.fileChannel.close();
            }
        }
    }

    public long getLastModifiedTimestamp() {
        return this.file.lastModified();
    }

    public int getFileSize() {
        return fileSize;
    }

    public FileChannel getFileChannel() {
        return fileChannel;
    }

    /**
     * 追加消息至MappFile中
     */
    public AppendMessageResult appendMessage(final MessageExtBrokerInner msg, final AppendMessageCallback cb) {
        return appendMessagesInner(msg, cb);
    }

    public AppendMessageResult appendMessages(final MessageExtBatch messageExtBatch, final AppendMessageCallback cb) {
        return appendMessagesInner(messageExtBatch, cb);
    }

    /**
     * 追加消息到MappedFile对应的文件中
     */
    public AppendMessageResult appendMessagesInner(final MessageExt messageExt, final AppendMessageCallback cb) {
        assert messageExt != null;
        assert cb != null;

        //获取MappedFile当前写指针
        int currentPos = this.wrotePosition.get();

        if (currentPos < this.fileSize) {
            //通过writeBuffer.slice()方法创建一个与原ByteBuffer共享的内存区，但拥有独立的position、limit、capacity等指针。并设置position指针
            // 如果writerBuffer不为空，说明开启了transientStorePoolEnable机制，则消息首先写入writerBuffer中，如果其为空，则写入mappedByteBuffer中。
            ByteBuffer byteBuffer = writeBuffer != null ? writeBuffer.slice() : this.mappedByteBuffer.slice();
            byteBuffer.position(currentPos);
            AppendMessageResult result = null;
            //追加消息指Buffer中，doAppend只是将消息追加至内存中，需要根据是同步刷盘还是异步刷盘方式，将内存中的数据持久化到磁盘。
            if (messageExt instanceof MessageExtBrokerInner) {
                result = cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos, (MessageExtBrokerInner) messageExt);
            } else if (messageExt instanceof MessageExtBatch) {
                result = cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos, (MessageExtBatch) messageExt);
            } else {
                return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
            }
            this.wrotePosition.addAndGet(result.getWroteBytes());
            this.storeTimestamp = result.getStoreTimestamp();
            return result;
        }
        //说明currentPos>=文件大小，抛异常。
        log.error("MappedFile.appendMessage return null, wrotePosition: {} fileSize: {}", currentPos, this.fileSize);
        return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
    }

    public long getFileFromOffset() {
        return this.fileFromOffset;
    }

    public boolean appendMessage(final byte[] data) {
        int currentPos = this.wrotePosition.get();

        if ((currentPos + data.length) <= this.fileSize) {
            try {
                this.fileChannel.position(currentPos);
                this.fileChannel.write(ByteBuffer.wrap(data));
            } catch (Throwable e) {
                log.error("Error occurred when append message to mappedFile.", e);
            }
            this.wrotePosition.addAndGet(data.length);
            return true;
        }

        return false;
    }

    /**
     * Content of data from offset to offset + length will be wrote to file.
     *
     * @param offset The offset of the subarray to be used.
     * @param length The length of the subarray to be used.
     */
    public boolean appendMessage(final byte[] data, final int offset, final int length) {
        int currentPos = this.wrotePosition.get();

        if ((currentPos + length) <= this.fileSize) {
            try {
                this.fileChannel.position(currentPos);
                this.fileChannel.write(ByteBuffer.wrap(data, offset, length));
            } catch (Throwable e) {
                log.error("Error occurred when append message to mappedFile.", e);
            }
            this.wrotePosition.addAndGet(length);
            return true;
        }

        return false;
    }

    /**
     * @return The current flushed position
     * 将内存中的数据刷写到磁盘，永久存储在磁盘中。
     * commitLeastPages为本次刷写的最小页数。
     *
     * 通过fileChannel.force()或者mappedByteBuffer.force()将内存中的数据持久化到磁盘
     */
    public int flush(final int flushLeastPages) {
        if (this.isAbleToFlush(flushLeastPages)) {
            if (this.hold()) {
                int value = getReadPosition();

                try {
                    //We only append data to fileChannel or mappedByteBuffer, never both.
                    if (writeBuffer != null || this.fileChannel.position() != 0) {
                        //刷盘
                        this.fileChannel.force(false);
                    } else {
                        //刷盘
                        this.mappedByteBuffer.force();
                    }
                } catch (Throwable e) {
                    log.error("Error occurred when force data to disk.", e);
                }

                /**
                 * 刷盘后，
                 * 1. {@link MappedFile#writeBuffer}不为空，数据是从{@link MappedFile#writeBuffer}进入到{@link MappedFile#fileChannel}
                 * flushedPosition应该等于writeBuffer的{@link MappedFile#committedPosition}，标识这些数据都已经提交到fileChannel中，并且刷盘了
                 * 2. writeBuffer为空，数据是直接进入到{@link MappedFile#mappedByteBuffer}，且从mappedByteBuffer中直接刷盘，
                 * flushedPosition应该等于mappedByteBuffer的写指针wrotePosition，标识写指针之前的数据都已经刷盘了。
                 *
                 * 所以更新flushedPosition的值为对应的指针。
                 */
                this.flushedPosition.set(value);
                this.release();
            } else {
                log.warn("in flush, hold failed, flush offset = " + this.flushedPosition.get());
                this.flushedPosition.set(getReadPosition());
            }
        }
        return this.getFlushedPosition();
    }

    /**
     * 执行提交操作。commitLeastPages为本次提交的最小页数。一页默认4k
     *
     * 将writeBuffer中的数据提交到文件通道FileChannel中。
     */
    public int commit(final int commitLeastPages) {
        if (writeBuffer == null) {
            //no need to commit data to file channel, so just regard wrotePosition as committedPosition.
            // 如果writeBuffer为空直接返回wrotePosition，无需commit，表明commit操作主体是writeBuffer
            return this.wrotePosition.get();
        }
        // 判断当前可提交数据页数是否大于等于commitLeastPages
        if (this.isAbleToCommit(commitLeastPages)) {
            if (this.hold()) {
                commit0(commitLeastPages);
                this.release();
            } else {
                log.warn("in commit, hold failed, commit offset = " + this.committedPosition.get());
            }
        }

        // All dirty data has been committed to FileChannel.
        //表明所有的脏数据都已经提交到FileChannel了，释放writeBuffer，可重用此部分堆外内存
        if (writeBuffer != null && this.transientStorePool != null && this.fileSize == this.committedPosition.get()) {
            this.transientStorePool.returnBuffer(writeBuffer);
            this.writeBuffer = null;
        }

        return this.committedPosition.get();
    }

    /**
     *
     * 将writewriteBuffer堆外内存中的数据写入fileChannel中
     */
    protected void commit0(final int commitLeastPages) {
        int writePos = this.wrotePosition.get();
        int lastCommittedPosition = this.committedPosition.get();

        if (writePos - this.committedPosition.get() > 0) {
            try {
                //首先创建writeBuffer共享缓存区。
                ByteBuffer byteBuffer = writeBuffer.slice();
                //将新的position回退到上一次的lastCommittedPosition
                byteBuffer.position(lastCommittedPosition);
                //limit设置为writePos，当前最大有效数据指针。
                byteBuffer.limit(writePos);
                this.fileChannel.position(lastCommittedPosition);
                //然后把lastCommittedPosition到writePos之前的数据写入fileChannel中（刷数据值PageCache系统中）
                this.fileChannel.write(byteBuffer);
                //更新committedPosition为writePos
                this.committedPosition.set(writePos);
            } catch (Throwable e) {
                log.error("Error occurred when commit data to FileChannel.", e);
            }
        }
    }

    private boolean isAbleToFlush(final int flushLeastPages) {
        //获取当前刷盘指针
        int flush = this.flushedPosition.get();
        //获取当前提交指针或者写指针
        int write = getReadPosition();

        //如果文件满了，返回true
        if (this.isFull()) {
            return true;
        }

        if (flushLeastPages > 0) {
            //当前可刷盘页面大于最少输盘页面数，则可以输盘。
            return ((write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE)) >= flushLeastPages;
        }

        // commitLeastPages <= 0标识只要存在脏数据就刷盘。
        return write > flush;
    }

    protected boolean isAbleToCommit(final int commitLeastPages) {
        int flush = this.committedPosition.get();
        int write = this.wrotePosition.get();

        //如果文件满了 那么肯定要提交
        if (this.isFull()) {
            return true;
        }

        //当前可提交页面大于最少提交页面数，则可以提交。
        if (commitLeastPages > 0) {
            return ((write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE)) >= commitLeastPages;
        }

        // commitLeastPages <= 0标识只要存在脏页就提交。
        return write > flush;
    }

    public int getFlushedPosition() {
        return flushedPosition.get();
    }

    public void setFlushedPosition(int pos) {
        this.flushedPosition.set(pos);
    }

    public boolean isFull() {
        return this.fileSize == this.wrotePosition.get();
    }

    /**
     * 查找pos开始的数据并且大小为size
     *
     * 消息读取时，是从mappedByteBuffer中读(pageCache)。
     * 而在{@link MappedFile#appendMessagesInner(org.apache.rocketmq.common.message.MessageExt, org.apache.rocketmq.store.AppendMessageCallback)}
     * 方法中如果开启了transientStorePoolEnable机制那么写入的是writeBuffer
     *
     * 所以如果开启transientStorePoolEnable机制，是不是有了读写分离的效果，先写入writerBuffer中，读却是从mappedByteBuffer中读取。
     */
    public SelectMappedBufferResult selectMappedBuffer(int pos, int size) {
        //获取当前文件最大的可读指针
        int readPosition = getReadPosition();
        //如果获取的最大数据偏移量小于等于readPosition，那么是可以获取到数据的。
        if ((pos + size) <= readPosition) {

            if (this.hold()) {
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            } else {
                log.warn("matched, but hold failed, request pos: " + pos + ", fileFromOffset: "
                    + this.fileFromOffset);
            }
        } else {
            log.warn("selectMappedBuffer request pos invalid, request pos: " + pos + ", size: " + size
                + ", fileFromOffset: " + this.fileFromOffset);
        }

        return null;
    }

    /**
     * 查找pos到当前最大可读之间的数据
     */
    public SelectMappedBufferResult selectMappedBuffer(int pos) {
        //获取当前文件最大的可读指针
        int readPosition = getReadPosition();
        if (pos < readPosition && pos >= 0) {
            if (this.hold()) {
                /**
                 * 由于在整个写入期间都未改变mappedByteBuffer的写指针，
                 * 所以mappedByteBuffer.slice()方法返回的共享缓存区间为整个MappedFile。
                 * 然后通过设置byteBuffer的position为待查找值。
                 * 所以读取的起始位置为pos
                 * limit为readPosition-pos
                 */
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                int size = readPosition - pos;
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            }
        }

        return null;
    }

    /**
     * 释放资源
     */
    @Override
    public boolean cleanup(final long currentRef) {
        //如果当前MappedFile可用，无须清理。
        if (this.isAvailable()) {
            log.error("this file[REF:" + currentRef + "] " + this.fileName
                + " have not shutdown, stop unmapping.");
            return false;
        }

        // 如果资源已经被清理，则返回true、
        if (this.isCleanupOver()) {
            log.error("this file[REF:" + currentRef + "] " + this.fileName
                + " have cleanup, do not do it again.");
            return true;
        }

        //清理Buffer
        clean(this.mappedByteBuffer);
        //维护下面两个变量值
        TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(this.fileSize * (-1));
        TOTAL_MAPPED_FILES.decrementAndGet();
        log.info("unmap file[REF:" + currentRef + "] " + this.fileName + " OK");
        return true;
    }

    /**
     * 由于使用了内存映射文件，只要存在于存储目录下的文件都需要对应创建内存映射文件，
     * 如果不定时将已消费的消息从存储文件中删除，会造成极大的内存压力与资源浪费，
     * 所以RocketMQ采取定时删除存储文件的策略。
     *
     * 释放MappedFile占用的内存资源（内存映射与内存通道等）
     * 1. 先释放资源，前提是MappedFile的org.apache.rocketmq.store.ReferenceResource#refCount引用小于等于0
     * 2. 关闭文件通道
     * 3. 删除物理文件
     *
     * @param intervalForcibly 拒绝被销毁的最大存活时间，现在的时间戳与第一次尝试shutdown的时间戳间隔如果大于intervalForcibly，那么及时当前文件还有引用，也强制销毁。
     *
     *
     */
    public boolean destroy(final long intervalForcibly) {
        //关闭MappedFile
        this.shutdown(intervalForcibly);

        //资源已经释放
        if (this.isCleanupOver()) {
            try {
                //关闭文件通道
                this.fileChannel.close();
                log.info("close file channel " + this.fileName + " OK");

                long beginTime = System.currentTimeMillis();
                //删除物理文件
                boolean result = this.file.delete();
                log.info("delete file[REF:" + this.getRefCount() + "] " + this.fileName
                    + (result ? " OK, " : " Failed, ") + "W:" + this.getWrotePosition() + " M:"
                    + this.getFlushedPosition() + ", "
                    + UtilAll.computeEclipseTimeMilliseconds(beginTime));
            } catch (Exception e) {
                log.warn("close file channel " + this.fileName + " Failed. ", e);
            }

            return true;
        } else {
            log.warn("destroy mapped file[REF:" + this.getRefCount() + "] " + this.fileName
                + " Failed. cleanupOver: " + this.cleanupOver);
        }

        return false;
    }

    public int getWrotePosition() {
        return wrotePosition.get();
    }

    public void setWrotePosition(int pos) {
        this.wrotePosition.set(pos);
    }

    /**
     * @return The max position which have valid data
     * 获取当前Buffer中最大的可读指针，相当于最大可刷盘指针，当沙盘时此指针前面的数据全部会被刷盘
     *
     * 1. {@link MappedFile#writeBuffer}不为空，数据是从{@link MappedFile#writeBuffer}进入到{@link MappedFile#fileChannel}
     * 当前文件最大的可读指针应该等于writeBuffer的{@link MappedFile#committedPosition}，标识这些数据都将会提交到fileChannel中，刷盘时会全部刷盘
     * 2. writeBuffer为空，数据是直接进入到{@link MappedFile#mappedByteBuffer}，且从mappedByteBuffer中直接刷盘，
     * 当前文件最大的可读指针应该等于mappedByteBuffer的写指针wrotePosition，标识写指针之前的数据都已经刷盘了。
     *
     * 在MappedFile的设计中，只有提交了的数据（写入到{@link MappedFile#mappedByteBuffer}或{@link MappedFile#fileChannel}）才是安全的数据
     */
    public int getReadPosition() {
        return this.writeBuffer == null ? this.wrotePosition.get() : this.committedPosition.get();
    }

    public void setCommittedPosition(int pos) {
        this.committedPosition.set(pos);
    }

    /**
     * 文件预热
     * mmap是一种将文件映射到虚拟内存的技术，可以将文件在磁盘位置的地址和在虚拟内存中的虚拟地址通过映射对应起来，
     * 之后就可以在内存这块区域进行读写数据，而不必调用系统级别的read,wirte这些函数，从而提升IO操作性能，
     * 另外一点就是mmap后的虚拟内存大小必须是内存页大小(通常是4K)的倍数，之所以这么做是为了匹配内存操作。
     *
     * 这里 MappedFile 已经创建，对应的 Buffer 为 mappedByteBuffer。
     * mappedByteBuffer 已经通过 mmap 映射，此时操作系统中只是记录了该文件和该 Buffer 的映射关系，而没有映射到物理内存中。
     * 这里就通过对该 MappedFile 的每个 Page Cache 进行写入一个字节，通过读写操作把 mmap 映射全部加载到物理内存中。
     */
    public void warmMappedFile(FlushDiskType type, int pages) {
        long beginTime = System.currentTimeMillis();
        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        int flush = 0;
        long time = System.currentTimeMillis();
        for (int i = 0, j = 0; i < this.fileSize; i += MappedFile.OS_PAGE_SIZE, j++) {
            byteBuffer.put(i, (byte) 0);
            // force flush when flush disk type is sync
            // 如果是同步写盘操作，则进行强行刷盘操作
            if (type == FlushDiskType.SYNC_FLUSH) {
                if ((i / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE) >= pages) {
                    flush = i;
                    mappedByteBuffer.force();
                }
            }

            // prevent gc
            if (j % 1000 == 0) {
                log.info("j={}, costTime={}", j, System.currentTimeMillis() - time);
                time = System.currentTimeMillis();
                try {
                    Thread.sleep(0);
                } catch (InterruptedException e) {
                    log.error("Interrupted", e);
                }
            }
        }

        // force flush when prepare load finished
        // 把剩余的数据强制刷新到磁盘中
        if (type == FlushDiskType.SYNC_FLUSH) {
            log.info("mapped file warm-up done, force to disk, mappedFile={}, costTime={}",
                this.getFileName(), System.currentTimeMillis() - beginTime);
            mappedByteBuffer.force();
        }
        log.info("mapped file warm-up done. mappedFile={}, costTime={}", this.getFileName(),
            System.currentTimeMillis() - beginTime);

        this.mlock();
    }

    public String getFileName() {
        return fileName;
    }

    public MappedByteBuffer getMappedByteBuffer() {
        return mappedByteBuffer;
    }

    public ByteBuffer sliceByteBuffer() {
        return this.mappedByteBuffer.slice();
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public boolean isFirstCreateInQueue() {
        return firstCreateInQueue;
    }

    public void setFirstCreateInQueue(boolean firstCreateInQueue) {
        this.firstCreateInQueue = firstCreateInQueue;
    }

    /**
     * 锁定内存
     * 该方法主要是实现文件预热后，防止把预热过的文件被操作系统调到swap空间中。当程序再次读取交换出去的数据的时候会产生缺页异常。
     */
    public void mlock() {
        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        {
            int ret = LibC.INSTANCE.mlock(pointer, new NativeLong(this.fileSize));
            log.info("mlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }

        {
            int ret = LibC.INSTANCE.madvise(pointer, new NativeLong(this.fileSize), LibC.MADV_WILLNEED);
            log.info("madvise {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }
    }

    /**
     * 销毁内存池
     */
    public void munlock() {
        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        int ret = LibC.INSTANCE.munlock(pointer, new NativeLong(this.fileSize));
        log.info("munlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
    }

    //testable
    File getFile() {
        return this.file;
    }

    @Override
    public String toString() {
        return this.fileName;
    }
}

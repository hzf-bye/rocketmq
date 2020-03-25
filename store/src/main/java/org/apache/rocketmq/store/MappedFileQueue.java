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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.config.MessageStoreConfig;

/**
 * mappedFileQueue可以看做是${ROCKETMQ_HOME}/store/commitlog文件夹，
 * 而MappedFile则对应该文件中的一个个文件
 *
 */
public class MappedFileQueue {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private static final InternalLogger LOG_ERROR = InternalLoggerFactory.getLogger(LoggerName.STORE_ERROR_LOGGER_NAME);

    /**
     * 每次删除的最大文件数
     */
    private static final int DELETE_FILES_BATCH_MAX = 10;

    /**
     * commitLog文件存储路径
     * @see MessageStoreConfig#storePathCommitLog
     */
    private final String storePath;

    /**
     * 每个commitLog文件大小，默认1G
     * @see MessageStoreConfig#mapedFileSizeCommitLog
     *
     * 每个consumeQueue文件大小，默认6000000 byte
     * {@link MessageStoreConfig#mapedFileSizeConsumeQueue}
     */
    private final int mappedFileSize;

    /**
     * mappedFiles文件命名规则。
     * 1.文件名为此文件中第一条消息的偏移量。
     * 2.第一个文件的偏移量为0，那么文件名即为00000000000000000000；
     *  第二个文件偏移量为0+mappedFileSize，假设mappedFileSize默认为1G，那么第二个文件文件名为0+1024*1024*1024即00000000001073741824
     *  依次类推第三个文件的文件名0+1024*1024*1024*1024*1024*1024即00000000002147483648。
     *  文件名最小20位数字组成不足的前面补零。
     * @see UtilAll#offset2FileName(long)
     */
    private final CopyOnWriteArrayList<MappedFile> mappedFiles = new CopyOnWriteArrayList<MappedFile>();

    /**
     * 创建MappedFile的服务类
     */
    private final AllocateMappedFileService allocateMappedFileService;

    /**
     * 当前刷盘指针，标识该指针之前的所有数据全部持久化到磁盘。
     */
    private long flushedWhere = 0;

    /**
     * 当前数据提交指针，内存中ByteBuffer当前的写指针，该值大于等于flushedWhere
     */
    private long committedWhere = 0;

    /**
     * commitlog文件中刷盘的最新一条消息的存储时间。
     */
    private volatile long storeTimestamp = 0;

    public MappedFileQueue(final String storePath, int mappedFileSize,
        AllocateMappedFileService allocateMappedFileService) {
        this.storePath = storePath;
        this.mappedFileSize = mappedFileSize;
        this.allocateMappedFileService = allocateMappedFileService;
    }

    public void checkSelf() {

        if (!this.mappedFiles.isEmpty()) {
            Iterator<MappedFile> iterator = mappedFiles.iterator();
            MappedFile pre = null;
            while (iterator.hasNext()) {
                MappedFile cur = iterator.next();

                if (pre != null) {
                    if (cur.getFileFromOffset() - pre.getFileFromOffset() != this.mappedFileSize) {
                        LOG_ERROR.error("[BUG]The mappedFile queue's data is damaged, the adjacent mappedFile's offset don't match. pre file {}, cur file {}",
                            pre.getFileName(), cur.getFileName());
                    }
                }
                pre = cur;
            }
        }
    }

    /**
     * 根据消息存储时间来查询MappedFile
     */
    public MappedFile getMappedFileByTime(final long timestamp) {
        //获取所有的MappedFile
        Object[] mfs = this.copyMappedFiles(0);

        //如果为空则返回空
        if (null == mfs)
            return null;

        //从mappedFiles列表中的第一个文件开始查找
        for (int i = 0; i < mfs.length; i++) {
            MappedFile mappedFile = (MappedFile) mfs[i];
            //找到第一个最后一次更新时间大于待查找时间戳的文件
            if (mappedFile.getLastModifiedTimestamp() >= timestamp) {
                return mappedFile;
            }
        }

        //如果不存在则返回最后一个文件
        return (MappedFile) mfs[mfs.length - 1];
    }

    private Object[] copyMappedFiles(final int reservedMappedFiles) {
        Object[] mfs;

        if (this.mappedFiles.size() <= reservedMappedFiles) {
            return null;
        }

        mfs = this.mappedFiles.toArray();
        return mfs;
    }

    /**
     * 删除offset对应的文件之后的所有文件
     * @param offset
     */
    public void truncateDirtyFiles(long offset) {
        List<MappedFile> willRemoveFiles = new ArrayList<MappedFile>();

        for (MappedFile file : this.mappedFiles) {
            long fileTailOffset = file.getFileFromOffset() + this.mappedFileSize;
            //fileTailOffset <= offset 则直接跳过
            if (fileTailOffset > offset) {
                //如果文件起始偏移量<= offser，且文件尾部偏移量 > offset，那么更新指针
                if (offset >= file.getFileFromOffset()) {
                    file.setWrotePosition((int) (offset % this.mappedFileSize));
                    file.setCommittedPosition((int) (offset % this.mappedFileSize));
                    file.setFlushedPosition((int) (offset % this.mappedFileSize));
                } else {
                    //说明这里的文件是offset对应的文件之后的文件，释放MappedFile占用的内存资源（内存映射与内存通道等）
                    file.destroy(1000);
                    //加入待移除文件列表
                    willRemoveFiles.add(file);
                }
            }
        }

        //已删除的文件从列表中移除，
        this.deleteExpiredFile(willRemoveFiles);
    }

    void deleteExpiredFile(List<MappedFile> files) {

        if (!files.isEmpty()) {

            Iterator<MappedFile> iterator = files.iterator();
            while (iterator.hasNext()) {
                MappedFile cur = iterator.next();
                if (!this.mappedFiles.contains(cur)) {
                    iterator.remove();
                    log.info("This mappedFile {} is not contained by mappedFiles, so skip it.", cur.getFileName());
                }
            }

            try {
                if (!this.mappedFiles.removeAll(files)) {
                    log.error("deleteExpiredFile remove failed.");
                }
            } catch (Exception e) {
                log.error("deleteExpiredFile has exception.", e);
            }
        }
    }

    /**
     * Broker启动时加载commitLog文件存储路径下的文件
     * @return
     */
    public boolean load() {
        File dir = new File(this.storePath);
        File[] files = dir.listFiles();
        if (files != null) {
            // ascending order
            // 根据文件名排序
            Arrays.sort(files);
            for (File file : files) {

                if (file.length() != this.mappedFileSize) {
                    log.warn(file + "\t" + file.length()
                        + " length not matched message store config value, ignore it");
                    return true;
                }

                try {
                    //创建MappedFile
                    MappedFile mappedFile = new MappedFile(file.getPath(), mappedFileSize);

                    //先初始化各个指针为文件大小，load完后，恢复commitlog、consumeQueue等文件的时候会重新计算各个指针
                    mappedFile.setWrotePosition(this.mappedFileSize);
                    mappedFile.setFlushedPosition(this.mappedFileSize);
                    mappedFile.setCommittedPosition(this.mappedFileSize);
                    this.mappedFiles.add(mappedFile);
                    log.info("load " + file.getPath() + " OK");
                } catch (IOException e) {
                    log.error("load file " + file + " error", e);
                    return false;
                }
            }
        }

        return true;
    }

    public long howMuchFallBehind() {
        if (this.mappedFiles.isEmpty())
            return 0;

        long committed = this.flushedWhere;
        if (committed != 0) {
            MappedFile mappedFile = this.getLastMappedFile(0, false);
            if (mappedFile != null) {
                return (mappedFile.getFileFromOffset() + mappedFile.getWrotePosition()) - committed;
            }
        }

        return 0;
    }

    /**
     * 获取最后一个MappedFile文件
     * @param startOffset 创建MappedFile文件起始偏移量
     * @param needCreate 如果当前不存在MapperFile文件或者MappedFile文件满了，是否需要创建新的文件
     * @return
     */
    public MappedFile getLastMappedFile(final long startOffset, boolean needCreate) {
        long createOffset = -1;
        MappedFile mappedFileLast = getLastMappedFile();

        //为空说明此时不存在commitLog文件
        if (mappedFileLast == null) {
            //计算文件起始位置，为了文件的起始偏移量为mappedFileSize的整数倍
            createOffset = startOffset - (startOffset % this.mappedFileSize);
        }

        //说明最后一个文件满了，那么要计算当前文件的偏移量
        if (mappedFileLast != null && mappedFileLast.isFull()) {
            createOffset = mappedFileLast.getFileFromOffset() + this.mappedFileSize;
        }

        if (createOffset != -1 && needCreate) {
            //计算当前文件的名字，为createOffset，不满20位则前面补0
            String nextFilePath = this.storePath + File.separator + UtilAll.offset2FileName(createOffset);
            //获取下一个文件的文件名，为当前文件第一条消息的偏移量+文件大小
            String nextNextFilePath = this.storePath + File.separator
                + UtilAll.offset2FileName(createOffset + this.mappedFileSize);
            MappedFile mappedFile = null;

            if (this.allocateMappedFileService != null) {
                mappedFile = this.allocateMappedFileService.putRequestAndReturnMappedFile(nextFilePath,
                    nextNextFilePath, this.mappedFileSize);
            } else {
                try {
                    //创建文件
                    mappedFile = new MappedFile(nextFilePath, this.mappedFileSize);
                } catch (IOException e) {
                    log.error("create mappedFile exception", e);
                }
            }

            if (mappedFile != null) {
                if (this.mappedFiles.isEmpty()) {
                    //标识为第一个创建的文件
                    mappedFile.setFirstCreateInQueue(true);
                }
                this.mappedFiles.add(mappedFile);
            }

            return mappedFile;
        }

        return mappedFileLast;
    }

    public MappedFile getLastMappedFile(final long startOffset) {
        return getLastMappedFile(startOffset, true);
    }

    public MappedFile getLastMappedFile() {
        MappedFile mappedFileLast = null;

        while (!this.mappedFiles.isEmpty()) {
            try {
                mappedFileLast = this.mappedFiles.get(this.mappedFiles.size() - 1);
                break;
            } catch (IndexOutOfBoundsException e) {
                //continue;
            } catch (Exception e) {
                log.error("getLastMappedFile has exception.", e);
                break;
            }
        }

        return mappedFileLast;
    }

    public boolean resetOffset(long offset) {
        MappedFile mappedFileLast = getLastMappedFile();

        if (mappedFileLast != null) {
            long lastOffset = mappedFileLast.getFileFromOffset() +
                mappedFileLast.getWrotePosition();
            long diff = lastOffset - offset;

            final int maxDiff = this.mappedFileSize * 2;
            if (diff > maxDiff)
                return false;
        }

        ListIterator<MappedFile> iterator = this.mappedFiles.listIterator();

        while (iterator.hasPrevious()) {
            mappedFileLast = iterator.previous();
            if (offset >= mappedFileLast.getFileFromOffset()) {
                int where = (int) (offset % mappedFileLast.getFileSize());
                mappedFileLast.setFlushedPosition(where);
                mappedFileLast.setWrotePosition(where);
                mappedFileLast.setCommittedPosition(where);
                break;
            } else {
                iterator.remove();
            }
        }
        return true;
    }

    /**
     * 获取存储文件最小偏移量。
     * 并不是直接返回0，原因在{@link MappedFileQueue#findMappedFileByOffset(long, boolean)}中已解释。
     */
    public long getMinOffset() {

        if (!this.mappedFiles.isEmpty()) {
            try {
                return this.mappedFiles.get(0).getFileFromOffset();
            } catch (IndexOutOfBoundsException e) {
                //continue;
            } catch (Exception e) {
                log.error("getMinOffset has exception.", e);
            }
        }
        return -1;
    }

    /**
     * 获取存储文件最大偏移量。
     * 返回最后一个MappedFile的fileFromOffset加上MappedFile文件当前写指针。
     */
    public long getMaxOffset() {
        MappedFile mappedFile = getLastMappedFile();
        if (mappedFile != null) {
            return mappedFile.getFileFromOffset() + mappedFile.getReadPosition();
        }
        return 0;
    }

    /**
     * 返回存储文件当前写指针。
     * 返回最后一个MappedFile的fileFromOffset加上MappedFile文件当前写指针。
     */
    public long getMaxWrotePosition() {
        MappedFile mappedFile = getLastMappedFile();
        if (mappedFile != null) {
            return mappedFile.getFileFromOffset() + mappedFile.getWrotePosition();
        }
        return 0;
    }

    public long remainHowManyDataToCommit() {
        return getMaxWrotePosition() - committedWhere;
    }

    public long remainHowManyDataToFlush() {
        return getMaxOffset() - flushedWhere;
    }

    public void deleteLastMappedFile() {
        MappedFile lastMappedFile = getLastMappedFile();
        if (lastMappedFile != null) {
            lastMappedFile.destroy(1000);
            this.mappedFiles.remove(lastMappedFile);
            log.info("on recover, destroy a logic mapped file " + lastMappedFile.getFileName());

        }
    }

    /**
     * 根据文件允许的最大存活时间删除commitlog文件
     * @param expiredTime {@link org.apache.rocketmq.store.config.MessageStoreConfig#fileReservedTime}
     * @param deleteFilesInterval {@link MessageStoreConfig#deleteCommitLogFilesInterval}
     * @param intervalForcibly {@link org.apache.rocketmq.store.config.MessageStoreConfig#destroyMapedFileIntervalForcibly}
     * @param cleanImmediately 是否强制删除文件（当磁盘使用超过设定的阈值）
     * @return 清理的文件数
     */
    public int deleteExpiredFileByTime(final long expiredTime,
        final int deleteFilesInterval,
        final long intervalForcibly,
        final boolean cleanImmediately) {
        //返回所有的mappedFile文件
        Object[] mfs = this.copyMappedFiles(0);

        if (null == mfs)
            return 0;

        int mfsLength = mfs.length - 1;
        int deleteCount = 0;
        List<MappedFile> files = new ArrayList<MappedFile>();
        if (null != mfs) {
            //从第一个文件遍历到倒数第二个文件。
            for (int i = 0; i < mfsLength; i++) {
                MappedFile mappedFile = (MappedFile) mfs[i];
                //计算文件最大存活时间（=文件最后一次更新时间+文件存活时间（默认72小时））
                long liveMaxTimestamp = mappedFile.getLastModifiedTimestamp() + expiredTime;
                //如果当前时间大于文件文件最大存活时间，或者需要强制删除（当磁盘使用超过设定的阈值）则清除MappedFile占有的相关资源。
                if (System.currentTimeMillis() >= liveMaxTimestamp || cleanImmediately) {
                    if (mappedFile.destroy(intervalForcibly)) {
                        //文件清除成功，加入待删除列表且deleteCount+1
                        files.add(mappedFile);
                        deleteCount++;

                        //如果删除的文件大小超过最大值，则不再删除。
                        if (files.size() >= DELETE_FILES_BATCH_MAX) {
                            break;
                        }

                        //继续执行删除下一个文件的操作时，先睡眠deleteFilesIntervalms
                        if (deleteFilesInterval > 0 && (i + 1) < mfsLength) {
                            try {
                                Thread.sleep(deleteFilesInterval);
                            } catch (InterruptedException e) {
                            }
                        }
                    } else {
                        break;
                    }
                } else {
                    //avoid deleting files in the middle
                    break;
                }
            }
        }

        deleteExpiredFile(files);

        return deleteCount;
    }


    /**
     * 根据CommitLog文件中的消息物理偏移量删除consumeQueue中的文件
     * @param offset CommitLog文件中的消息物理偏移量
     * @param unitSize 目前consumeQueue中的文件的一个条目大小，20字节
     * @return
     */
    public int deleteExpiredFileByOffset(long offset, int unitSize) {
        //返回所有的mappedFile文件
        Object[] mfs = this.copyMappedFiles(0);

        List<MappedFile> files = new ArrayList<MappedFile>();
        int deleteCount = 0;
        if (null != mfs) {

            int mfsLength = mfs.length - 1;

            //从第一个文件遍历到倒数第二个文件。
            for (int i = 0; i < mfsLength; i++) {
                boolean destroy;
                MappedFile mappedFile = (MappedFile) mfs[i];
                //获取当前文件最后一个条目的物理偏移量
                SelectMappedBufferResult result = mappedFile.selectMappedBuffer(this.mappedFileSize - unitSize);
                if (result != null) {
                    //获取当前条目消息的物理偏移量。
                    long maxOffsetInLogicQueue = result.getByteBuffer().getLong();
                    result.release();
                    //当前文件最有一个条目的消息的物理偏移量 < offset则表示需要清理
                    destroy = maxOffsetInLogicQueue < offset;
                    if (destroy) {
                        log.info("physic min offset " + offset + ", logics in current mappedFile max offset "
                            + maxOffsetInLogicQueue + ", delete it");
                    }
                } else if (!mappedFile.isAvailable()) { // Handle hanged file.
                    log.warn("Found a hanged consume queue file, attempting to delete it.");
                    destroy = true;
                } else {
                    log.warn("this being not executed forever.");
                    break;
                }

                //清理文件
                if (destroy && mappedFile.destroy(1000 * 60)) {
                    files.add(mappedFile);
                    deleteCount++;
                } else {
                    break;
                }
            }
        }

        deleteExpiredFile(files);

        return deleteCount;
    }

    /**
     *
     * @return true-此次没有数据提交，false-此次有数据提交成功
     */
    public boolean flush(final int flushLeastPages) {
        boolean result = true;
        MappedFile mappedFile = this.findMappedFileByOffset(this.flushedWhere, this.flushedWhere == 0);
        if (mappedFile != null) {
            long tmpTimeStamp = mappedFile.getStoreTimestamp();
            // offset标识刷盘之后的刷盘指针
            int offset = mappedFile.flush(flushLeastPages);
            long where = mappedFile.getFileFromOffset() + offset;
            /*
             * 如果此时result为false，表示 where != this.committedWhere，也就表示此次有数据提交成功
             * 反之result为true，表示此次没有数据提交。
             */
            result = where == this.flushedWhere;
            this.flushedWhere = where;
            if (0 == flushLeastPages) {
                this.storeTimestamp = tmpTimeStamp;
            }
        }

        return result;
    }

    /**
     *
     * @param commitLeastPages {@link MessageStoreConfig#commitCommitLogLeastPages}
     * @return true-此次没有数据提交，false-此次有数据提交成功
     */
    public boolean commit(final int commitLeastPages) {
        boolean result = true;
        MappedFile mappedFile = this.findMappedFileByOffset(this.committedWhere, this.committedWhere == 0);
        if (mappedFile != null) {
            int offset = mappedFile.commit(commitLeastPages);
            long where = mappedFile.getFileFromOffset() + offset;
            /*
             * 如果此时result为false，表示 where != this.committedWhere，也就表示此次有数据提交成功
             * 反之result为true，表示此次没有数据提交。
             */
            result = where == this.committedWhere;
            this.committedWhere = where;
        }

        return result;
    }

    /**
     * 根据消息偏移量查找MappedFile
     * Finds a mapped file by offset.
     *
     * 根据offset%this.mappedFileSize是否可能？
     * 答案是否定的，由于使用了内存映射文件，只要存在于存储目录下的文件都需要对应创建内存映射文件，如果不定时将已消费的消息从存储文件中删除，
     * 会造成极大的内存压力与资源浪费，所以RocketMQ采取定时删除存储文件的策略。也就是说在存储文件中，第一个文件不一定是00000000000000000000，
     * 因为该文件在某一个时刻会被删除。故根据offset定位MappedFile的算法为int index = (int) ((offset / this.mappedFileSize) - (firstMappedFile.getFileFromOffset() / this.mappedFileSize));
     * 举个例子：
     * this.mappedFileSize = 10。
     * 存储文件中存在的文件为20、30、40
     * 此时offset=35
     * 那么根据上面算法计算出来的index = 1;
     * 因此在{@link MappedFileQueue#mappedFiles}中找下标为1的MappedFile即可。
     *
     * @param offset Offset.
     * @param returnFirstOnNotFound If the mapped file is not found, then return the first one.
     * @return Mapped file or null (when not found and returnFirstOnNotFound is <code>false</code>).
     */
    public MappedFile findMappedFileByOffset(final long offset, final boolean returnFirstOnNotFound) {
        try {
            MappedFile firstMappedFile = this.getFirstMappedFile();
            MappedFile lastMappedFile = this.getLastMappedFile();
            if (firstMappedFile != null && lastMappedFile != null) {
                //判断消息偏移量是否小于第一个MappedFile或者大于最后一个MappedFile的偏移量
                if (offset < firstMappedFile.getFileFromOffset() || offset >= lastMappedFile.getFileFromOffset() + this.mappedFileSize) {
                    //没有此偏移量的文件则打印日志
                    LOG_ERROR.warn("Offset not matched. Request offset: {}, firstOffset: {}, lastOffset: {}, mappedFileSize: {}, mappedFiles count: {}",
                        offset,
                        firstMappedFile.getFileFromOffset(),
                        lastMappedFile.getFileFromOffset() + this.mappedFileSize,
                        this.mappedFileSize,
                        this.mappedFiles.size());
                } else {
                    //存在此偏移量的文件
                    // 根据下面算法获得此偏移量的文件下标
                    int index = (int) ((offset / this.mappedFileSize) - (firstMappedFile.getFileFromOffset() / this.mappedFileSize));
                    MappedFile targetFile = null;
                    try {
                        targetFile = this.mappedFiles.get(index);
                    } catch (Exception ignored) {
                    }

                    if (targetFile != null && offset >= targetFile.getFileFromOffset()
                        && offset < targetFile.getFileFromOffset() + this.mappedFileSize) {
                        return targetFile;
                    }

                    /*
                     * 如果根据上面算法还未找到则直接遍历mappedFile文件
                     * 什么情况下会来到这儿，目前还不知道？ 在我看来mappedFile文件名是个等差数组，公差为this.mappedFileSize
                     * 估计出现某种异常情况才会走到这里？
                     */
                    for (MappedFile tmpMappedFile : this.mappedFiles) {
                        if (offset >= tmpMappedFile.getFileFromOffset()
                            && offset < tmpMappedFile.getFileFromOffset() + this.mappedFileSize) {
                            return tmpMappedFile;
                        }
                    }
                }

                //如果没有找到目标mappedFile，是否需要返回第一个mappedFile。
                if (returnFirstOnNotFound) {
                    return firstMappedFile;
                }
            }
        } catch (Exception e) {
            log.error("findMappedFileByOffset Exception", e);
        }

        return null;
    }

    public MappedFile getFirstMappedFile() {
        MappedFile mappedFileFirst = null;

        if (!this.mappedFiles.isEmpty()) {
            try {
                mappedFileFirst = this.mappedFiles.get(0);
            } catch (IndexOutOfBoundsException e) {
                //ignore
            } catch (Exception e) {
                log.error("getFirstMappedFile has exception.", e);
            }
        }

        return mappedFileFirst;
    }

    public MappedFile findMappedFileByOffset(final long offset) {
        return findMappedFileByOffset(offset, false);
    }

    public long getMappedMemorySize() {
        long size = 0;

        Object[] mfs = this.copyMappedFiles(0);
        if (mfs != null) {
            for (Object mf : mfs) {
                if (((ReferenceResource) mf).isAvailable()) {
                    size += this.mappedFileSize;
                }
            }
        }

        return size;
    }

    public boolean retryDeleteFirstFile(final long intervalForcibly) {
        MappedFile mappedFile = this.getFirstMappedFile();
        if (mappedFile != null) {
            if (!mappedFile.isAvailable()) {
                log.warn("the mappedFile was destroyed once, but still alive, " + mappedFile.getFileName());
                boolean result = mappedFile.destroy(intervalForcibly);
                if (result) {
                    log.info("the mappedFile re delete OK, " + mappedFile.getFileName());
                    List<MappedFile> tmpFiles = new ArrayList<MappedFile>();
                    tmpFiles.add(mappedFile);
                    this.deleteExpiredFile(tmpFiles);
                } else {
                    log.warn("the mappedFile re delete failed, " + mappedFile.getFileName());
                }

                return result;
            }
        }

        return false;
    }

    public void shutdown(final long intervalForcibly) {
        for (MappedFile mf : this.mappedFiles) {
            mf.shutdown(intervalForcibly);
        }
    }

    public void destroy() {
        for (MappedFile mf : this.mappedFiles) {
            mf.destroy(1000 * 3);
        }
        this.mappedFiles.clear();
        this.flushedWhere = 0;

        // delete parent directory
        File file = new File(storePath);
        if (file.isDirectory()) {
            file.delete();
        }
    }

    public long getFlushedWhere() {
        return flushedWhere;
    }

    public void setFlushedWhere(long flushedWhere) {
        this.flushedWhere = flushedWhere;
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public List<MappedFile> getMappedFiles() {
        return mappedFiles;
    }

    public int getMappedFileSize() {
        return mappedFileSize;
    }

    public long getCommittedWhere() {
        return committedWhere;
    }

    public void setCommittedWhere(final long committedWhere) {
        this.committedWhere = committedWhere;
    }
}

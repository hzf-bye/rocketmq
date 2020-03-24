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
package org.apache.rocketmq.store.index;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.List;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.MappedFile;
import org.apache.rocketmq.store.config.MessageStoreConfig;

/**
 * IndexFile文件默认存储格式
 * indexHeader(40字节){@link IndexFile#indexHeader}+500万个hash槽(每个槽4个字节)+2000万个Index条目
 */
public class IndexFile {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    /**
     * IndexFile中每个槽占4个字节
     * 存储该hash槽对应的Index条目索引
     */
    private static int hashSlotSize = 4;
    private static int indexSize = 20;
    private static int invalidIndex = 0;
    /**
     * 一个IndexFile默认包含500万个Hash槽，每个Hash槽存储的是落在该Hash槽的hashCode最新的index索引，相当于2000万个index条目中的下标
     * {@link MessageStoreConfig#maxHashSlotNum}
     * 每个hash槽存储的是落在该槽的hashcode对应的最新的index索引
     */
    private final int hashSlotNum;
    /**
     * 一个IndexFile默认包含2000万个条目
     * {@link MessageStoreConfig#maxIndexNum}
     */
    private final int indexNum;
    private final MappedFile mappedFile;
    private final FileChannel fileChannel;
    /**
     * index条目列表。默认一个索引文件包含20000万个条目，每一个Index条目20字节结构如下：
     * hashCode：4个字节，key的hashCode
     * phyoffset：8个字节，消息对应的物理偏移量。
     * timedif：4个字节，该消息存储时间与第一条消息的时间戳的差值，小于0该消息无效。
     * preIndexNo：4个字节，该条目的前一条记录的index列表中的Index索引（可以理解为数组下标），当出现hash冲突时，构建的链表结构。
     *
     * mappedByteBuffer的存储格式为
     * indexHeader(40字节){@link IndexFile#indexHeader}+500万个hash槽(每个槽4个字节)+2000万个Index条目
     *
     */
    private final MappedByteBuffer mappedByteBuffer;

    /**
     * 包含40个字节，记录该IndexFile的统计信息
     * 8字节beginTimestamp+8字节endTimestamp+8字节beginPhyOffset+8字节endPhyOffset+4字节hashSlotCount+4字节indexCount
     */
    private final IndexHeader indexHeader;

    /**
     *
     * @param fileName 文件名
     * @param hashSlotNum 最大hash槽数量
     * @param indexNum 最大索引条目数量
     * @param endPhyOffset 若是第一个索引文件，则endPhyOffset=0；否则为上一个索引文件的最大消息偏移量
     * @param endTimestamp 若是第一个索引文件，则endTimestamp=0；否则为上一个索引文件的最大消息存储时间
     * @throws IOException
     */
    public IndexFile(final String fileName, final int hashSlotNum, final int indexNum,
        final long endPhyOffset, final long endTimestamp) throws IOException {
        int fileTotalSize =
            IndexHeader.INDEX_HEADER_SIZE + (hashSlotNum * hashSlotSize) + (indexNum * indexSize);
        this.mappedFile = new MappedFile(fileName, fileTotalSize);
        this.fileChannel = this.mappedFile.getFileChannel();
        this.mappedByteBuffer = this.mappedFile.getMappedByteBuffer();
        this.hashSlotNum = hashSlotNum;
        this.indexNum = indexNum;

        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        this.indexHeader = new IndexHeader(byteBuffer);

        //这里虽然会初始化下面四个变量，但是在写入消息索引信息时会覆盖下面信息
        if (endPhyOffset > 0) {
            this.indexHeader.setBeginPhyOffset(endPhyOffset);
            this.indexHeader.setEndPhyOffset(endPhyOffset);
        }

        if (endTimestamp > 0) {
            this.indexHeader.setBeginTimestamp(endTimestamp);
            this.indexHeader.setEndTimestamp(endTimestamp);
        }
    }

    public String getFileName() {
        return this.mappedFile.getFileName();
    }

    public void load() {
        this.indexHeader.load();
    }

    public void flush() {
        long beginTime = System.currentTimeMillis();
        if (this.mappedFile.hold()) {
            this.indexHeader.updateByteBuffer();
            this.mappedByteBuffer.force();
            this.mappedFile.release();
            log.info("flush index file eclipse time(ms) " + (System.currentTimeMillis() - beginTime));
        }
    }

    public boolean isWriteFull() {
        return this.indexHeader.getIndexCount() >= this.indexNum;
    }

    public boolean destroy(final long intervalForcibly) {
        return this.mappedFile.destroy(intervalForcibly);
    }

    /**
     * 将消息索引键与消息偏移量映射关系写入到IndexFile的实现方法。
     * @param key 消息索引
     * @param phyOffset 消息物理偏移量
     * @param storeTimestamp 消息存储时间
     * @return
     */
    public boolean putKey(final String key, final long phyOffset, final long storeTimestamp) {
        //如果当前已经使用的条目大于最大条目数是，返回false，表示当前索引文件已经写满。
        if (this.indexHeader.getIndexCount() < this.indexNum) {
            //根据key计算出hashCode，之所以存储使用hashcode而不是用key，是为了将Index条目设计成定长结构，方面检索与定位条目。
            int keyHash = indexKeyHashMethod(key);
            //计算出keyHash对应的hash槽下标
            int slotPos = keyHash % this.hashSlotNum;
            //对应的hash槽的物理地址为IndexHeader头部40个字节+slotPos*hashSlotSize(每个槽大小)
            int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;

            FileLock fileLock = null;

            try {

                // fileLock = this.fileChannel.lock(absSlotPos, hashSlotSize,
                // false);
                //读取当前获得的hash槽对应的数据
                int slotValue = this.mappedByteBuffer.getInt(absSlotPos);
                //如果小于等于0或者大于当前索引文件中的索引条目数目，则将slotValue设置为0，说明此时槽中没有对应的index条目，也就是没有发生hash冲突。
                if (slotValue <= invalidIndex || slotValue > this.indexHeader.getIndexCount()) {
                    slotValue = invalidIndex;
                }

                //计算待存储消息的时间戳与第一条消息的存储时间戳差值，并转换为秒
                long timeDiff = storeTimestamp - this.indexHeader.getBeginTimestamp();

                timeDiff = timeDiff / 1000;

                if (this.indexHeader.getBeginTimestamp() <= 0) {
                    timeDiff = 0;
                } else if (timeDiff > Integer.MAX_VALUE) {
                    timeDiff = Integer.MAX_VALUE;
                } else if (timeDiff < 0) {
                    timeDiff = 0;
                }

                //将条目信息存储在IndexFile中
                //计算当前需要添加的条目的起始偏移量。
                int absIndexPos =
                    IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * hashSlotSize
                        + this.indexHeader.getIndexCount() * indexSize;

                //依次将hashcode、消息物理偏移量、消息存储时间戳、索引文件时间戳、当前Hash槽的值存入mappedByteBuffer中
                this.mappedByteBuffer.putInt(absIndexPos, keyHash);
                this.mappedByteBuffer.putLong(absIndexPos + 4, phyOffset);
                this.mappedByteBuffer.putInt(absIndexPos + 4 + 8, (int) timeDiff);
                //当前条目计算当前条目添加之前的hash槽的值，相当于hash冲突时组成一个单向链表。
                //如果slotValue=0那么说明没有发生hash冲突，即当前新增的条目为hash槽中的第一个元素。
                this.mappedByteBuffer.putInt(absIndexPos + 4 + 8 + 4, slotValue);

                /*
                 * 应当注意到this.indexHeader.getIndexCount()的初始值为1，那么说明index条目列表中下标为0的索引一直是空的。
                 * 因此当添加第一个元素时就是落在index条目列表中下标为1的位置。
                 * 因此获取当前的this.indexHeader.getIndexCount()的值即为当前新增条目的索引。
                 *
                 * 举个例子，假设当前是一次新增index索引，
                 * 那么this.indexHeader.getIndexCount() = 1,
                 * 所以此时hash槽中存储的是1
                 *
                 *
                 * 至于为什么这么设计
                 * 我的理解是因为hash槽中存储的是index条目索引，而hash槽中如果值为0又代表了当前没有与之对应的index索引(invalidIndex = 0)，即没有发生hash冲突
                 * 所以这里特意把index条目索引为0的避开了。即索引为0的空间不存储数据。
                 */
                this.mappedByteBuffer.putInt(absSlotPos, this.indexHeader.getIndexCount());

                //当前条目小于等于1，说明此时新增的条目为第一个条目，因为indexCount默认值为1
                if (this.indexHeader.getIndexCount() <= 1) {
                    this.indexHeader.setBeginPhyOffset(phyOffset);
                    this.indexHeader.setBeginTimestamp(storeTimestamp);
                }

                this.indexHeader.incHashSlotCount();
                this.indexHeader.incIndexCount();
                this.indexHeader.setEndPhyOffset(phyOffset);
                this.indexHeader.setEndTimestamp(storeTimestamp);

                return true;
            } catch (Exception e) {
                log.error("putKey exception, Key: " + key + " KeyHashCode: " + key.hashCode(), e);
            } finally {
                if (fileLock != null) {
                    try {
                        fileLock.release();
                    } catch (IOException e) {
                        log.error("Failed to release the lock", e);
                    }
                }
            }
        } else {
            log.warn("Over index file capacity: index count = " + this.indexHeader.getIndexCount()
                + "; index max num = " + this.indexNum);
        }

        return false;
    }

    public int indexKeyHashMethod(final String key) {
        int keyHash = key.hashCode();
        int keyHashPositive = Math.abs(keyHash);
        if (keyHashPositive < 0)
            keyHashPositive = 0;
        return keyHashPositive;
    }

    public long getBeginTimestamp() {
        return this.indexHeader.getBeginTimestamp();
    }

    public long getEndTimestamp() {
        return this.indexHeader.getEndTimestamp();
    }

    public long getEndPhyOffset() {
        return this.indexHeader.getEndPhyOffset();
    }

    public boolean isTimeMatched(final long begin, final long end) {
        boolean result = begin < this.indexHeader.getBeginTimestamp() && end > this.indexHeader.getEndTimestamp();
        result = result || (begin >= this.indexHeader.getBeginTimestamp() && begin <= this.indexHeader.getEndTimestamp());
        result = result || (end >= this.indexHeader.getBeginTimestamp() && end <= this.indexHeader.getEndTimestamp());
        return result;
    }

    /**
     * 根据索引key查找消息
     * @param phyOffsets 查找到的消息物理偏移量
     * @param key 索引key
     * @param maxNum 本次查找最大消息条数
     * @param begin 开始时间戳
     * @param end 结束时间戳
     * @param lock
     */
    public void selectPhyOffset(final List<Long> phyOffsets, final String key, final int maxNum,
        final long begin, final long end, boolean lock) {
        if (this.mappedFile.hold()) {
            //根据 key计算出hashcode
            int keyHash = indexKeyHashMethod(key);
            //计算出hash槽下标
            int slotPos = keyHash % this.hashSlotNum;
            //获取hash槽的物理地址
            int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;

            FileLock fileLock = null;
            try {
                if (lock) {
                    // fileLock = this.fileChannel.lock(absSlotPos,
                    // hashSlotSize, true);
                }

                //获取槽中对应的index条目的索引
                int slotValue = this.mappedByteBuffer.getInt(absSlotPos);
                // if (fileLock != null) {
                // fileLock.release();
                // fileLock = null;
                // }

                //满足下述条件之一标识hashCode没有对应的条目，结束查找
                if (slotValue <= invalidIndex || slotValue > this.indexHeader.getIndexCount()
                    || this.indexHeader.getIndexCount() <= 1) {
                } else {
                    for (int nextIndexToRead = slotValue; ; ) {
                        //找到了期望的条数的消息结束循环
                        if (phyOffsets.size() >= maxNum) {
                            break;
                        }

                        //获取当前index条目的物理偏移量
                        int absIndexPos =
                            IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * hashSlotSize
                                + nextIndexToRead * indexSize;

                        //获取对应的hashcode
                        int keyHashRead = this.mappedByteBuffer.getInt(absIndexPos);
                        //获取消息的物理偏移量
                        long phyOffsetRead = this.mappedByteBuffer.getLong(absIndexPos + 4);

                        //获取与第一条消息的时间戳的差值，单位秒
                        long timeDiff = (long) this.mappedByteBuffer.getInt(absIndexPos + 4 + 8);
                        //获取上一个消息的index索引
                        int prevIndexRead = this.mappedByteBuffer.getInt(absIndexPos + 4 + 8 + 4);
                        //时间差值小于0，直接结束循环
                        if (timeDiff < 0) {
                            break;
                        }

                        //转化为毫秒
                        timeDiff *= 1000L;

                        //获取当前index条目对应的消息的存储时间
                        long timeRead = this.indexHeader.getBeginTimestamp() + timeDiff;
                        //消息存储时间介于二者之间那么匹配
                        boolean timeMatched = (timeRead >= begin) && (timeRead <= end);

                        //hashcode匹配且时间匹配那么加入到phyOffsets中
                        if (keyHash == keyHashRead && timeMatched) {
                            phyOffsets.add(phyOffsetRead);
                        }

                        //判断是否有上一条消息的存在。
                        if (prevIndexRead <= invalidIndex
                            || prevIndexRead > this.indexHeader.getIndexCount()
                            || prevIndexRead == nextIndexToRead || timeRead < begin) {
                            break;
                        }

                        nextIndexToRead = prevIndexRead;
                    }
                }
            } catch (Exception e) {
                log.error("selectPhyOffset exception ", e);
            } finally {
                if (fileLock != null) {
                    try {
                        fileLock.release();
                    } catch (IOException e) {
                        log.error("Failed to release the lock", e);
                    }
                }

                this.mappedFile.release();
            }
        }
    }
}

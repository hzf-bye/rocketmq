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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.ha.HAService;
import org.apache.rocketmq.store.schedule.ScheduleMessageService;

/**
 * Store all metadata downtime for recovery, data protection reliability
 * commitlog文件默认存储路径为${ROCKETMQ_HOME}/store/commitlog
 * Broker启动时可以通过-c 命令指定存储路径。
 * 每个文件默认1G，一个文件写满后在创建下一个，以该文件中第一个偏移量为文件名，偏移量小于20位用0补齐。
 * 比如有两个文件，名字分别问00000000000000000000与00000000001073741824。
 * 00000000000000000000：表示第文件中第一条消息的物理偏移量为0；
 * 00000000001392038478：表示第文件中第一条消息的物理偏移量为1392038478。
 * 这样根据物理偏移量能快速定位到消息。
 */
public class CommitLog {
    // Message's MAGIC CODE daa320a7
    // 正常的消息对应的魔数
    public final static int MESSAGE_MAGIC_CODE = -626843481;
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    // End of file empty MAGIC CODE cbd43194
    /**
     * 当文件剩余空间不够写入某条消息时，
     * 那么最有8个有效字节，前四个写入当前剩余的空白空间，
     * 后四个字节写入BLANK_MAGIC_CODE
     * {@link DefaultAppendMessageCallback#doAppend(long, java.nio.ByteBuffer, int, org.apache.rocketmq.store.MessageExtBrokerInner)}
     */
    private final static int BLANK_MAGIC_CODE = -875286124;
    /**
     * mappedFileQueue
     * mappedFileQueue可以看做是${ROCKETMQ_HOME}/store/commitlog文件夹，
     * 而MappedFile则对应该文件中的一个个文件
     */
    private final MappedFileQueue mappedFileQueue;
    private final DefaultMessageStore defaultMessageStore;
    private final FlushCommitLogService flushCommitLogService;

    //If TransientStorePool enabled, we must flush message to FileChannel at fixed periods
    private final FlushCommitLogService commitLogService;

    private final AppendMessageCallback appendMessageCallback;
    private final ThreadLocal<MessageExtBatchEncoder> batchEncoderThreadLocal;
    /**
     * 当有消息需要追加至commitLog中时回来此获取消息消费队列逻辑偏移量(即消息消费队列中的最大索引/下标)
     * 当某个topic-queueid第一次获取时获取到null，会初始化为0，然后追加完消息后value值加1
     * 即每次获取到的值即为当前消息在消息对应的消费队列文件中（topic/queueid文件夹中的）的对应逻辑偏移量。
     * {@link DefaultAppendMessageCallback#doAppend(long, java.nio.ByteBuffer, int, org.apache.rocketmq.common.message.MessageExtBatch)}
     * 将此偏移量存储在消息中，那么在消息转发时可以直接获取到此消息在消息消费队列文件中的物理偏移量。
     *
     * key:topic-queueid
     * value:下一次追加消息时，消息在ConsumeQueue文件中的逻辑偏移量。
     */
    private HashMap<String/* topic-queueid */, Long/* offset */> topicQueueTable = new HashMap<String, Long>(1024);
    /**
     * CommitLog提交指针
     */
    private volatile long confirmOffset = -1L;

    /**
     * 获取到锁的时间
     *
     * 因为将消息写入CommitLog中时串行的
     * 因此在写入消息时需要先获取所{@link CommitLog#putMessageLock}
     * 获取到了那么设置beginTimeInLock为当前时间，
     * 处理完本次消息后不管成功还是失败 那么beginTimeInLock都将设为初始值0
     *
     * 所以beginTimeInLock==0时表示当前没有消息写入
     *
     * 处理逻辑见：
     * {@link org.apache.rocketmq.store.CommitLog#putMessage(org.apache.rocketmq.store.MessageExtBrokerInner)}
     */
    private volatile long beginTimeInLock = 0;
    private final PutMessageLock putMessageLock;

    public CommitLog(final DefaultMessageStore defaultMessageStore) {
        this.mappedFileQueue = new MappedFileQueue(defaultMessageStore.getMessageStoreConfig().getStorePathCommitLog(),
            defaultMessageStore.getMessageStoreConfig().getMapedFileSizeCommitLog(), defaultMessageStore.getAllocateMappedFileService());
        this.defaultMessageStore = defaultMessageStore;

        if (FlushDiskType.SYNC_FLUSH == defaultMessageStore.getMessageStoreConfig().getFlushDiskType()) {
            this.flushCommitLogService = new GroupCommitService();
        } else {
            this.flushCommitLogService = new FlushRealTimeService();
        }

        this.commitLogService = new CommitRealTimeService();

        this.appendMessageCallback = new DefaultAppendMessageCallback(defaultMessageStore.getMessageStoreConfig().getMaxMessageSize());
        batchEncoderThreadLocal = new ThreadLocal<MessageExtBatchEncoder>() {
            @Override
            protected MessageExtBatchEncoder initialValue() {
                return new MessageExtBatchEncoder(defaultMessageStore.getMessageStoreConfig().getMaxMessageSize());
            }
        };
        this.putMessageLock = defaultMessageStore.getMessageStoreConfig().isUseReentrantLockWhenPutMessage() ? new PutMessageReentrantLock() : new PutMessageSpinLock();

    }

    /**
     * Broker启动时加载CommitLog文件
     */
    public boolean load() {
        boolean result = this.mappedFileQueue.load();
        log.info("load commit log " + (result ? "OK" : "Failed"));
        return result;
    }

    public void start() {
        //启动刷盘线程
        this.flushCommitLogService.start();

        if (defaultMessageStore.getMessageStoreConfig().isTransientStorePoolEnable()) {
            this.commitLogService.start();
        }
    }

    public void shutdown() {
        if (defaultMessageStore.getMessageStoreConfig().isTransientStorePoolEnable()) {
            this.commitLogService.shutdown();
        }

        this.flushCommitLogService.shutdown();
    }

    public long flush() {
        this.mappedFileQueue.commit(0);
        this.mappedFileQueue.flush(0);
        return this.mappedFileQueue.getFlushedWhere();
    }

    public long getMaxOffset() {
        return this.mappedFileQueue.getMaxOffset();
    }

    public long remainHowManyDataToCommit() {
        return this.mappedFileQueue.remainHowManyDataToCommit();
    }

    public long remainHowManyDataToFlush() {
        return this.mappedFileQueue.remainHowManyDataToFlush();
    }

    public int deleteExpiredFile(
        final long expiredTime,
        final int deleteFilesInterval,
        final long intervalForcibly,
        final boolean cleanImmediately
    ) {
        return this.mappedFileQueue.deleteExpiredFileByTime(expiredTime, deleteFilesInterval, intervalForcibly, cleanImmediately);
    }

    /**
     * Read CommitLog data, use data replication
     */
    public SelectMappedBufferResult getData(final long offset) {
        return this.getData(offset, offset == 0);
    }

    public SelectMappedBufferResult getData(final long offset, final boolean returnFirstOnNotFound) {
        int mappedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMapedFileSizeCommitLog();
        MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset, returnFirstOnNotFound);
        if (mappedFile != null) {
            int pos = (int) (offset % mappedFileSize);
            SelectMappedBufferResult result = mappedFile.selectMappedBuffer(pos);
            return result;
        }

        return null;
    }

    /**
     * When the normal exit, data recovery, all memory data have been flush
     * Broker正常停止文件恢复
     * 主要是为了恢复当前CommitLog的提交指针、刷盘指针等。
     */
    public void recoverNormally() {
        //在进行文件恢复查找消息时是否验证CRC
        boolean checkCRCOnRecover = this.defaultMessageStore.getMessageStoreConfig().isCheckCRCOnRecover();
        final List<MappedFile> mappedFiles = this.mappedFileQueue.getMappedFiles();
        //从倒数第三个文件开始恢复，不足3个文件则从第一个开始恢复
        if (!mappedFiles.isEmpty()) {
            // Began to recover from the last third file
            int index = mappedFiles.size() - 3;
            if (index < 0)
                index = 0;

            MappedFile mappedFile = mappedFiles.get(index);
            ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
            //CommitLog已经确认过的物理偏移量等于mappedFile.getFileFromOffset()+mappedFileOffset
            long processOffset = mappedFile.getFileFromOffset();
            //当前文件已经校验过的offset（即校验过的所有消息的大小）
            long mappedFileOffset = 0;
            while (true) {
                DispatchRequest dispatchRequest = this.checkMessageAndReturnSize(byteBuffer, checkCRCOnRecover);
                int size = dispatchRequest.getMsgSize();
                // Normal data
                if (dispatchRequest.isSuccess() && size > 0) {
                    mappedFileOffset += size;
                }
                // Come the end of the file, switch to the next file Since the
                // return 0 representatives met last hole,
                // this can not be included in truncate offset
                //读取到的消息大小等于0，说明当前文件读取完了，原因详见doAppend方法
                else if (dispatchRequest.isSuccess() && size == 0) {
                    index++;
                    if (index >= mappedFiles.size()) {
                        // Current branch can not happen
                        log.info("recover last 3 physics file over, last mapped file " + mappedFile.getFileName());
                        break;
                    } else {
                        mappedFile = mappedFiles.get(index);
                        byteBuffer = mappedFile.sliceByteBuffer();
                        processOffset = mappedFile.getFileFromOffset();
                        mappedFileOffset = 0;
                        log.info("recover next physics file, " + mappedFile.getFileName());
                    }
                }
                // Intermediate file read error
                //解析某条消息失败，那么跳出循环，结束遍历恢复
                else if (!dispatchRequest.isSuccess()) {
                    log.info("recover physics file end, " + mappedFile.getFileName());
                    break;
                }
            }

            processOffset += mappedFileOffset;
            //更新刷盘指针与提交指针
            this.mappedFileQueue.setFlushedWhere(processOffset);
            this.mappedFileQueue.setCommittedWhere(processOffset);
            //更新processOffset对应的文件的写指针、提交指针、刷盘指针。
            //以及删除之后的所有文件
            this.mappedFileQueue.truncateDirtyFiles(processOffset);
        }
    }

    public DispatchRequest checkMessageAndReturnSize(java.nio.ByteBuffer byteBuffer, final boolean checkCRC) {
        return this.checkMessageAndReturnSize(byteBuffer, checkCRC, true);
    }

    private void doNothingForDeadCode(final Object obj) {
        if (obj != null) {
            log.debug(String.valueOf(obj.hashCode()));
        }
    }

    /**
     * check the message and returns the message size
     *
     * @return 0 Come the end of the file // >0 Normal messages // -1 Message checksum failure
     *
     * 校验消息，并在byteBuffer中解析一条消息
     */
    public DispatchRequest checkMessageAndReturnSize(java.nio.ByteBuffer byteBuffer, final boolean checkCRC,
        final boolean readBody) {
        try {
            // 1 TOTAL SIZE 获取消息大小
            int totalSize = byteBuffer.getInt();

            // 2 MAGIC CODE 模式
            int magicCode = byteBuffer.getInt();
            switch (magicCode) {
                case MESSAGE_MAGIC_CODE:
                    break;
                case BLANK_MAGIC_CODE:
                    return new DispatchRequest(0, true /* success */);
                default:
                    log.warn("found a illegal magic code 0x" + Integer.toHexString(magicCode));
                    return new DispatchRequest(-1, false /* success */);
            }

            //创建字节数组存储消息
            byte[] bytesContent = new byte[totalSize];

            //消息体crc检验码
            int bodyCRC = byteBuffer.getInt();

            //消息队列id
            int queueId = byteBuffer.getInt();

            //消息Flag，RocketMQ不处理，供程序使用
            int flag = byteBuffer.getInt();

            //消息在消息消费队列的下标
            long queueOffset = byteBuffer.getLong();

            //消息在CommitLog文件中的偏移量
            long physicOffset = byteBuffer.getLong();

            //消息系统FLAG，例如是否压缩、是否是事务消息等
            int sysFlag = byteBuffer.getInt();

            // 消息生产者调用消息发送API的时间戳
            long bornTimeStamp = byteBuffer.getLong();

            //消息发送者ip+端口
            ByteBuffer byteBuffer1 = byteBuffer.get(bytesContent, 0, 8);

            //消息存储时间
            long storeTimestamp = byteBuffer.getLong();

            //Broker服务器ip+端口
            ByteBuffer byteBuffer2 = byteBuffer.get(bytesContent, 0, 8);

            //消息重试次数
            int reconsumeTimes = byteBuffer.getInt();

            //事务消息物理偏移量
            long preparedTransactionOffset = byteBuffer.getLong();

            //消息体长度
            int bodyLen = byteBuffer.getInt();
            if (bodyLen > 0) {
                if (readBody) {
                    //获取消息体
                    byteBuffer.get(bytesContent, 0, bodyLen);

                    if (checkCRC) {
                        int crc = UtilAll.crc32(bytesContent, 0, bodyLen);
                        if (crc != bodyCRC) {
                            log.warn("CRC check failed. bodyCRC={}, currentCRC={}", crc, bodyCRC);
                            return new DispatchRequest(-1, false/* success */);
                        }
                    }
                } else {
                    byteBuffer.position(byteBuffer.position() + bodyLen);
                }
            }

            //主题存储长度
            byte topicLen = byteBuffer.get();
            //获取消息topic
            byteBuffer.get(bytesContent, 0, topicLen);
            String topic = new String(bytesContent, 0, topicLen, MessageDecoder.CHARSET_UTF8);

            long tagsCode = 0;
            String keys = "";
            String uniqKey = null;

            //消息属性长度
            short propertiesLength = byteBuffer.getShort();
            Map<String, String> propertiesMap = null;
            if (propertiesLength > 0) {
                //获取消息属性
                byteBuffer.get(bytesContent, 0, propertiesLength);
                String properties = new String(bytesContent, 0, propertiesLength, MessageDecoder.CHARSET_UTF8);
                propertiesMap = MessageDecoder.string2messageProperties(properties);

                keys = propertiesMap.get(MessageConst.PROPERTY_KEYS);

                uniqKey = propertiesMap.get(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);

                String tags = propertiesMap.get(MessageConst.PROPERTY_TAGS);
                if (tags != null && tags.length() > 0) {
                    tagsCode = MessageExtBrokerInner.tagsString2tagsCode(MessageExt.parseTopicFilterType(sysFlag), tags);
                }

                // Timing message processing
                {
                    String t = propertiesMap.get(MessageConst.PROPERTY_DELAY_TIME_LEVEL);
                    if (ScheduleMessageService.SCHEDULE_TOPIC.equals(topic) && t != null) {
                        int delayLevel = Integer.parseInt(t);

                        if (delayLevel > this.defaultMessageStore.getScheduleMessageService().getMaxDelayLevel()) {
                            delayLevel = this.defaultMessageStore.getScheduleMessageService().getMaxDelayLevel();
                        }

                        if (delayLevel > 0) {
                            tagsCode = this.defaultMessageStore.getScheduleMessageService().computeDeliverTimestamp(delayLevel,
                                storeTimestamp);
                        }
                    }
                }
            }

            int readLength = calMsgLength(bodyLen, topicLen, propertiesLength);
            if (totalSize != readLength) {
                doNothingForDeadCode(reconsumeTimes);
                doNothingForDeadCode(flag);
                doNothingForDeadCode(bornTimeStamp);
                doNothingForDeadCode(byteBuffer1);
                doNothingForDeadCode(byteBuffer2);
                log.error(
                    "[BUG]read total count not equals msg total size. totalSize={}, readTotalCount={}, bodyLen={}, topicLen={}, propertiesLength={}",
                    totalSize, readLength, bodyLen, topicLen, propertiesLength);
                return new DispatchRequest(totalSize, false/* success */);
            }

            return new DispatchRequest(
                topic,
                queueId,
                physicOffset,
                totalSize,
                tagsCode,
                storeTimestamp,
                queueOffset,
                keys,
                uniqKey,
                sysFlag,
                preparedTransactionOffset,
                propertiesMap
            );
        } catch (Exception e) {
        }

        return new DispatchRequest(-1, false /* success */);
    }

    /**
     * CommitLog条目是不定长的，每一个消息条目长度存储在前四个自己中
     */
    private static int calMsgLength(int bodyLength, int topicLength, int propertiesLength) {
        //RocketMQ消息存储格式如下
        final int msgLen = 4 //TOTALSIZE 该消息总长度四个字节
            + 4 //MAGICCODE 魔术四个字节。固定值 0xdaa3207a
            + 4 //BODYCRC 消息体crc检验码，4个字节
            + 4 //QUEUEID 消息消费队列，4字节
            + 4 //FLAG 消息Flag，RocketMQ不处理，供程序使用，默认4字节
            + 8 //QUEUEOFFSET 消息在消息消费队列的偏移量 8字节
            + 8 //PHYSICALOFFSET 消息在CommitLog文件中的偏移量，8字节
            + 4 //SYSFLAG 消息系统FLAG，例如是否压缩、是否是事务消息等，4字节
            + 8 //BORNTIMESTAMP 消息生产者调用消息发送API的时间戳，8字节
            + 8 //BORNHOST 消息发送者ip+端口，8字节
            + 8 //STORETIMESTAMP 消息存储时间戳，8字节
            + 8 //STOREHOSTADDRESS Broker服务器ip+端口，8字节
            + 4 //RECONSUMETIMES 消息重试次数，4字节
            + 8 //Prepared Transaction Offset 事务消息物理偏移量，8字节
            + 4 + (bodyLength > 0 ? bodyLength : 0) //BODY 消息体长度 4字节 + 消息体内容，长度为bodyLength
            + 1 + topicLength //TOPIC 主题存储长度 1字节，标识主题名称不能超过255字节 + 主题，长度为topicLength
            + 2 + (propertiesLength > 0 ? propertiesLength : 0) //propertiesLength 消息属性长度，2字节，标识消息属性不能超过65536个字符 + 消息属性，长度为propertiesLength
            + 0;
        return msgLen;
    }

    public long getConfirmOffset() {
        return this.confirmOffset;
    }

    public void setConfirmOffset(long phyOffset) {
        this.confirmOffset = phyOffset;
    }

    /**
     * Broker异常停止文件恢复
     */
    public void recoverAbnormally() {
        // recover by the minimum time stamp
        boolean checkCRCOnRecover = this.defaultMessageStore.getMessageStoreConfig().isCheckCRCOnRecover();
        final List<MappedFile> mappedFiles = this.mappedFileQueue.getMappedFiles();
        if (!mappedFiles.isEmpty()) {
            // Looking beginning to recover from which file
            //从最后一个文件往前走
            int index = mappedFiles.size() - 1;
            MappedFile mappedFile = null;
            for (; index >= 0; index--) {
                mappedFile = mappedFiles.get(index);
                //找到第一个消息存储正常的文件
                if (this.isMappedFileMatchedRecover(mappedFile)) {
                    log.info("recover from this mapped file " + mappedFile.getFileName());
                    break;
                }
            }

            //如果没有找到消息存储正常的文件，则从第一个开始
            if (index < 0) {
                index = 0;
                mappedFile = mappedFiles.get(index);
            }

            ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
            //CommitLog已经确认过的物理偏移量等于mappedFile.getFileFromOffset()+mappedFileOffset
            long processOffset = mappedFile.getFileFromOffset();
            //当前文件已经校验过的offset（即校验过的所有消息的大小）
            long mappedFileOffset = 0;
            while (true) {
                DispatchRequest dispatchRequest = this.checkMessageAndReturnSize(byteBuffer, checkCRCOnRecover);
                int size = dispatchRequest.getMsgSize();

                // Normal data
                // size > 0 说明是正常消息
                if (size > 0) {
                    mappedFileOffset += size;

                    //转发消息至消息消费队列文件（ConsumeQueue）、索引文件（IndexFile）的文件，判断是够索引允许重复构建
                    if (this.defaultMessageStore.getMessageStoreConfig().isDuplicationEnable()) {
                        if (dispatchRequest.getCommitLogOffset() < this.defaultMessageStore.getConfirmOffset()) {
                            this.defaultMessageStore.doDispatch(dispatchRequest);
                        }
                    } else {
                        this.defaultMessageStore.doDispatch(dispatchRequest);
                    }
                }
                // Intermediate file read error
                //解析消息异常，结束
                else if (size == -1) {
                    log.info("recover physics file end, " + mappedFile.getFileName());
                    break;
                }
                // Come the end of the file, switch to the next file
                // Since the return 0 representatives met last hole, this can
                // not be included in truncate offset
                // //读取到的消息大小等于0，说明当前文件读取完了，原因详见doAppend方法
                else if (size == 0) {
                    index++;
                    if (index >= mappedFiles.size()) {
                        // The current branch under normal circumstances should
                        // not happen
                        log.info("recover physics file over, last mapped file " + mappedFile.getFileName());
                        break;
                    } else {
                        mappedFile = mappedFiles.get(index);
                        byteBuffer = mappedFile.sliceByteBuffer();
                        processOffset = mappedFile.getFileFromOffset();
                        mappedFileOffset = 0;
                        log.info("recover next physics file, " + mappedFile.getFileName());
                    }
                }
            }

            processOffset += mappedFileOffset;
            //更新刷盘指针与提交指针
            this.mappedFileQueue.setFlushedWhere(processOffset);
            this.mappedFileQueue.setCommittedWhere(processOffset);
            //更新processOffset对应的文件的写指针、提交指针、刷盘指针。
            //以及删除之后的所有文件
            this.mappedFileQueue.truncateDirtyFiles(processOffset);

            // Clear ConsumeQueue redundant data
            // 根据processOffset截取消息消费队列文件
            this.defaultMessageStore.truncateDirtyLogicFiles(processOffset);
        }
        // Commitlog case files are deleted
        else {
            // 如果没有MappedFile文件，则初始化为0
            this.mappedFileQueue.setFlushedWhere(0);
            this.mappedFileQueue.setCommittedWhere(0);
            //销毁所有的消息消费队列文件
            this.defaultMessageStore.destroyLogics();
        }
    }

    private boolean isMappedFileMatchedRecover(final MappedFile mappedFile) {
        ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();

        // 判断第一条消息魔数，如果不是MESSAGE_MAGIC_CODE，返回false，表明该文件不符合commitlog消息文件的存储格式
        int magicCode = byteBuffer.getInt(MessageDecoder.MESSAGE_MAGIC_CODE_POSTION);
        if (magicCode != MESSAGE_MAGIC_CODE) {
            return false;
        }

        // 如果文件第一条消息存储时间等于0，返回false，表明该文件中未存储任何消息。
        long storeTimestamp = byteBuffer.getLong(MessageDecoder.MESSAGE_STORE_TIMESTAMP_POSTION);
        if (0 == storeTimestamp) {
            return false;
        }


        /*
         * 对比文件第一条消息的时间戳与监测点，文件第一条消息的时间戳小于文件监测点，说明该文件部分消息是可靠的，则从该文件开始恢复。
         * 文件监测点中保存了CommitLog文件、消息消费队列文件（ConsumeQueue）、索引文件（IndexFile）的文件刷盘点，
         * RocketMQ默认选择消息文件与消息消费队列文件这两个文件的刷盘点中较小值与消息文件第一条消息的消息戳作对比，如果messageIndexEnable为true，
         * 则索引文件的刷盘点也参与计算。
         */
        //this.defaultMessageStore.getMessageStoreConfig().isMessageIndexSafe()为false
        if (this.defaultMessageStore.getMessageStoreConfig().isMessageIndexEnable()
            && this.defaultMessageStore.getMessageStoreConfig().isMessageIndexSafe()) {
            if (storeTimestamp <= this.defaultMessageStore.getStoreCheckpoint().getMinTimestampIndex()) {
                log.info("find check timestamp, {} {}",
                    storeTimestamp,
                    UtilAll.timeMillisToHumanString(storeTimestamp));
                return true;
            }
        } else {
            if (storeTimestamp <= this.defaultMessageStore.getStoreCheckpoint().getMinTimestamp()) {
                log.info("find check timestamp, {} {}",
                    storeTimestamp,
                    UtilAll.timeMillisToHumanString(storeTimestamp));
                return true;
            }
        }

        return false;
    }

    private void notifyMessageArriving() {

    }

    public boolean resetOffset(long offset) {
        return this.mappedFileQueue.resetOffset(offset);
    }

    public long getBeginTimeInLock() {
        return beginTimeInLock;
    }

    public PutMessageResult putMessage(final MessageExtBrokerInner msg) {
        // Set the storage time
        msg.setStoreTimestamp(System.currentTimeMillis());
        // Set the message body BODY CRC (consider the most appropriate setting
        // on the client)
        //循环冗余校验CRC
        msg.setBodyCRC(UtilAll.crc32(msg.getBody()));
        // Back to Results
        AppendMessageResult result = null;

        StoreStatsService storeStatsService = this.defaultMessageStore.getStoreStatsService();

        String topic = msg.getTopic();
        int queueId = msg.getQueueId();

        //获取此消息是否是事务消息
        final int tranType = MessageSysFlag.getTransactionValue(msg.getSysFlag());
        if (tranType == MessageSysFlag.TRANSACTION_NOT_TYPE
            || tranType == MessageSysFlag.TRANSACTION_COMMIT_TYPE) {
            //如果不是事务消息或者是事务COMMIT消息
            // Delay Delivery
            /*
             * 如果是延迟的延迟级别大于0，即需要延迟发送消息
             * 延迟级别{@link MessageStoreConfig#messageDelayLevel}
             */
            if (msg.getDelayTimeLevel() > 0) {
                //如果消息延迟级别大于目前所支持的最大延迟级别，那么就设置为目前所支持的最大延迟级别
                if (msg.getDelayTimeLevel() > this.defaultMessageStore.getScheduleMessageService().getMaxDelayLevel()) {
                    msg.setDelayTimeLevel(this.defaultMessageStore.getScheduleMessageService().getMaxDelayLevel());
                }

                //使用延迟消息主题SCHEDULE_TOPIC
                topic = ScheduleMessageService.SCHEDULE_TOPIC;
                //消息队列id设置为消息延迟级别-1
                queueId = ScheduleMessageService.delayLevel2QueueId(msg.getDelayTimeLevel());

                // Backup real topic, queueId
                //消息扩展属性中存储真实的消息topic与队列id
                MessageAccessor.putProperty(msg, MessageConst.PROPERTY_REAL_TOPIC, msg.getTopic());
                MessageAccessor.putProperty(msg, MessageConst.PROPERTY_REAL_QUEUE_ID, String.valueOf(msg.getQueueId()));
                msg.setPropertiesString(MessageDecoder.messageProperties2String(msg.getProperties()));

                msg.setTopic(topic);
                msg.setQueueId(queueId);
            }
        }

        long eclipseTimeInLock = 0;
        MappedFile unlockMappedFile = null;

        /*
         * 获取当前可写入的commitlog文件，commitlog文件默认存储路径为${ROCKETMQ_HOME}/store/commitlog
         * Broker启动时可以通过-c 命令指定存储路径。
         * 每个文件默认1G，一个文件写满后在创建下一个，以该文件中第一个偏移量为文件名，偏移量小于20位用0补齐。
         * 比如有两个文件，名字分别问00000000000000000000与00000000001073741824。
         * 00000000000000000000：表示第文件中第一条消息的物理偏移量为0；
         * 00000000001392038478：表示第文件中第一条消息的物理偏移量为1392038478。
         * 这样根据物理偏移量能快速定位到消息。
         *
         */
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();

        //在写入CommitLog之前先申请putMessageLock，也就是将消息存储到CommitLog中是串行的
        putMessageLock.lock(); //spin or ReentrantLock ,depending on store config
        try {
            //设置消息存储时间
            long beginLockTimestamp = this.defaultMessageStore.getSystemClock().now();
            this.beginTimeInLock = beginLockTimestamp;

            // Here settings are stored timestamp, in order to ensure an orderly
            // global
            msg.setStoreTimestamp(beginLockTimestamp);

            /*
             * 1.如果mappedFile为空，表明${ROCKETMQ_HOME}/store/commitlog中不存在任何文件，
             * 说明本次消息是第一次发送消息，用偏移量0创建第一个commit文件，文件为00000000000000000000，
             * 2.如果mappedFile满了。同样的用偏移量0创建第一个commit文件，文件为00000000000000000000，
             */
            if (null == mappedFile || mappedFile.isFull()) {
                mappedFile = this.mappedFileQueue.getLastMappedFile(0); // Mark: NewFile may be cause noise
            }
            //如果文件创建失败则报错，很可能是磁盘空间不足或者权限不够。
            if (null == mappedFile) {
                log.error("create mapped file1 error, topic: " + msg.getTopic() + " clientAddr: " + msg.getBornHostString());
                beginTimeInLock = 0;
                return new PutMessageResult(PutMessageStatus.CREATE_MAPEDFILE_FAILED, null);
            }

            //将消息追加到mappedFile中
            result = mappedFile.appendMessage(msg, this.appendMessageCallback);
            switch (result.getStatus()) {
                case PUT_OK:
                    break;
                case END_OF_FILE:
                    //当前mappedFile文件满了，或者没有空间存下当前消息，那么创建再创建一个mappedFile文件存储。
                    unlockMappedFile = mappedFile;
                    // Create a new file, re-write the message
                    mappedFile = this.mappedFileQueue.getLastMappedFile(0);
                    if (null == mappedFile) {
                        // XXX: warn and notify me
                        log.error("create mapped file2 error, topic: " + msg.getTopic() + " clientAddr: " + msg.getBornHostString());
                        beginTimeInLock = 0;
                        return new PutMessageResult(PutMessageStatus.CREATE_MAPEDFILE_FAILED, result);
                    }
                    result = mappedFile.appendMessage(msg, this.appendMessageCallback);
                    break;
                case MESSAGE_SIZE_EXCEEDED:
                case PROPERTIES_SIZE_EXCEEDED:
                    //消息体或者消息属性过大
                    beginTimeInLock = 0;
                    return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, result);
                case UNKNOWN_ERROR:
                    beginTimeInLock = 0;
                    return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result);
                default:
                    beginTimeInLock = 0;
                    return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result);
            }

            eclipseTimeInLock = this.defaultMessageStore.getSystemClock().now() - beginLockTimestamp;
            beginTimeInLock = 0;
        } finally {
            putMessageLock.unlock();
        }

        if (eclipseTimeInLock > 500) {
            log.warn("[NOTIFYME]putMessage in lock cost time(ms)={}, bodyLength={} AppendMessageResult={}", eclipseTimeInLock, msg.getBody().length, result);
        }

        if (null != unlockMappedFile && this.defaultMessageStore.getMessageStoreConfig().isWarmMapedFileEnable()) {
            this.defaultMessageStore.unlockMappedFile(unlockMappedFile);
        }

        PutMessageResult putMessageResult = new PutMessageResult(PutMessageStatus.PUT_OK, result);

        // Statistics
        //记录当前topic存储次数与写入的消息大小
        storeStatsService.getSinglePutMessageTopicTimesTotal(msg.getTopic()).incrementAndGet();
        storeStatsService.getSinglePutMessageTopicSizeTotal(topic).addAndGet(result.getWroteBytes());

        //处理消息刷盘
        handleDiskFlush(result, putMessageResult, msg);
        handleHA(result, putMessageResult, msg);

        return putMessageResult;
    }

    public void handleDiskFlush(AppendMessageResult result, PutMessageResult putMessageResult, MessageExt messageExt) {
        // Synchronization flush
        // 同步刷盘
        if (FlushDiskType.SYNC_FLUSH == this.defaultMessageStore.getMessageStoreConfig().getFlushDiskType()) {
            final GroupCommitService service = (GroupCommitService) this.flushCommitLogService;
            //同步刷盘的策略是否等消息存储完成后再返回结果
            if (messageExt.isWaitStoreMsgOK()) {
                //构建GroupCommitRequest，
                GroupCommitRequest request = new GroupCommitRequest(result.getWroteOffset() + result.getWroteBytes());
                service.putRequest(request);
                //等待同步刷盘任务完成，如果超时返回刷盘错误
                boolean flushOK = request.waitForFlush(this.defaultMessageStore.getMessageStoreConfig().getSyncFlushTimeout());
                if (!flushOK) {
                    log.error("do groupcommit, wait for flush failed, topic: " + messageExt.getTopic() + " tags: " + messageExt.getTags()
                        + " client address: " + messageExt.getBornHostString());
                    putMessageResult.setPutMessageStatus(PutMessageStatus.FLUSH_DISK_TIMEOUT);
                }
            } else {
                // 直接唤醒等到的线程，不等到消息存储完成直接返回
                service.wakeup();
            }
        }
        // Asynchronous flush
        // 异步刷盘
        else {
            //根据是否开启transientStorePoolEnable机制，输盘实现会有细微差别。
            /**
             * 开启transientStorePoolEnable机制，详见{@link TransientStorePool}
             */
            if (!this.defaultMessageStore.getMessageStoreConfig().isTransientStorePoolEnable()) {
                flushCommitLogService.wakeup();
            } else {
                commitLogService.wakeup();
            }
        }
    }

    public void handleHA(AppendMessageResult result, PutMessageResult putMessageResult, MessageExt messageExt) {
        if (BrokerRole.SYNC_MASTER == this.defaultMessageStore.getMessageStoreConfig().getBrokerRole()) {
            HAService service = this.defaultMessageStore.getHaService();
            if (messageExt.isWaitStoreMsgOK()) {
                // Determine whether to wait
                if (service.isSlaveOK(result.getWroteOffset() + result.getWroteBytes())) {
                    GroupCommitRequest request = new GroupCommitRequest(result.getWroteOffset() + result.getWroteBytes());
                    service.putRequest(request);
                    service.getWaitNotifyObject().wakeupAll();
                    boolean flushOK =
                        request.waitForFlush(this.defaultMessageStore.getMessageStoreConfig().getSyncFlushTimeout());
                    if (!flushOK) {
                        log.error("do sync transfer other node, wait return, but failed, topic: " + messageExt.getTopic() + " tags: "
                            + messageExt.getTags() + " client address: " + messageExt.getBornHostNameString());
                        putMessageResult.setPutMessageStatus(PutMessageStatus.FLUSH_SLAVE_TIMEOUT);
                    }
                }
                // Slave problem
                else {
                    // Tell the producer, slave not available
                    putMessageResult.setPutMessageStatus(PutMessageStatus.SLAVE_NOT_AVAILABLE);
                }
            }
        }

    }

    public PutMessageResult putMessages(final MessageExtBatch messageExtBatch) {
        messageExtBatch.setStoreTimestamp(System.currentTimeMillis());
        AppendMessageResult result;

        StoreStatsService storeStatsService = this.defaultMessageStore.getStoreStatsService();

        final int tranType = MessageSysFlag.getTransactionValue(messageExtBatch.getSysFlag());

        if (tranType != MessageSysFlag.TRANSACTION_NOT_TYPE) {
            return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null);
        }
        if (messageExtBatch.getDelayTimeLevel() > 0) {
            return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null);
        }

        long eclipseTimeInLock = 0;
        MappedFile unlockMappedFile = null;
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();

        //fine-grained lock instead of the coarse-grained
        MessageExtBatchEncoder batchEncoder = batchEncoderThreadLocal.get();

        messageExtBatch.setEncodedBuff(batchEncoder.encode(messageExtBatch));

        putMessageLock.lock();
        try {
            long beginLockTimestamp = this.defaultMessageStore.getSystemClock().now();
            this.beginTimeInLock = beginLockTimestamp;

            // Here settings are stored timestamp, in order to ensure an orderly
            // global
            messageExtBatch.setStoreTimestamp(beginLockTimestamp);

            if (null == mappedFile || mappedFile.isFull()) {
                mappedFile = this.mappedFileQueue.getLastMappedFile(0); // Mark: NewFile may be cause noise
            }
            if (null == mappedFile) {
                log.error("Create mapped file1 error, topic: {} clientAddr: {}", messageExtBatch.getTopic(), messageExtBatch.getBornHostString());
                beginTimeInLock = 0;
                return new PutMessageResult(PutMessageStatus.CREATE_MAPEDFILE_FAILED, null);
            }

            result = mappedFile.appendMessages(messageExtBatch, this.appendMessageCallback);
            switch (result.getStatus()) {
                case PUT_OK:
                    break;
                case END_OF_FILE:
                    unlockMappedFile = mappedFile;
                    // Create a new file, re-write the message
                    mappedFile = this.mappedFileQueue.getLastMappedFile(0);
                    if (null == mappedFile) {
                        // XXX: warn and notify me
                        log.error("Create mapped file2 error, topic: {} clientAddr: {}", messageExtBatch.getTopic(), messageExtBatch.getBornHostString());
                        beginTimeInLock = 0;
                        return new PutMessageResult(PutMessageStatus.CREATE_MAPEDFILE_FAILED, result);
                    }
                    result = mappedFile.appendMessages(messageExtBatch, this.appendMessageCallback);
                    break;
                case MESSAGE_SIZE_EXCEEDED:
                case PROPERTIES_SIZE_EXCEEDED:
                    beginTimeInLock = 0;
                    return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, result);
                case UNKNOWN_ERROR:
                    beginTimeInLock = 0;
                    return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result);
                default:
                    beginTimeInLock = 0;
                    return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result);
            }

            eclipseTimeInLock = this.defaultMessageStore.getSystemClock().now() - beginLockTimestamp;
            beginTimeInLock = 0;
        } finally {
            putMessageLock.unlock();
        }

        if (eclipseTimeInLock > 500) {
            log.warn("[NOTIFYME]putMessages in lock cost time(ms)={}, bodyLength={} AppendMessageResult={}", eclipseTimeInLock, messageExtBatch.getBody().length, result);
        }

        if (null != unlockMappedFile && this.defaultMessageStore.getMessageStoreConfig().isWarmMapedFileEnable()) {
            this.defaultMessageStore.unlockMappedFile(unlockMappedFile);
        }

        PutMessageResult putMessageResult = new PutMessageResult(PutMessageStatus.PUT_OK, result);

        // Statistics
        storeStatsService.getSinglePutMessageTopicTimesTotal(messageExtBatch.getTopic()).addAndGet(result.getMsgNum());
        storeStatsService.getSinglePutMessageTopicSizeTotal(messageExtBatch.getTopic()).addAndGet(result.getWroteBytes());

        handleDiskFlush(result, putMessageResult, messageExtBatch);

        handleHA(result, putMessageResult, messageExtBatch);

        return putMessageResult;
    }

    /**
     * According to receive certain message or offset storage time if an error occurs, it returns -1
     */
    public long pickupStoreTimestamp(final long offset, final int size) {
        if (offset >= this.getMinOffset()) {
            SelectMappedBufferResult result = this.getMessage(offset, size);
            if (null != result) {
                try {
                    return result.getByteBuffer().getLong(MessageDecoder.MESSAGE_STORE_TIMESTAMP_POSTION);
                } finally {
                    result.release();
                }
            }
        }

        return -1;
    }

    /**
     * 获取当前commitlog目录中最小偏移量。
     */
    public long getMinOffset() {
        //获取目录下的第一个文件。
        MappedFile mappedFile = this.mappedFileQueue.getFirstMappedFile();
        if (mappedFile != null) {
            if (mappedFile.isAvailable()) {
                //如果该文件可用则直接返回起始偏移量
                return mappedFile.getFileFromOffset();
            } else {
                //否则返回下一个文件的起始偏移量。
                return this.rollNextFile(mappedFile.getFileFromOffset());
            }
        }

        return -1;
    }

    /**
     * 根据消息偏移量与消息长度查找消息。
     *
     * 如果只根据消息查找一条消息，则首先获取到文件内的偏移量，然后读取4个字节获取消息总长度，然后读取指定字节即可。
     */
    public SelectMappedBufferResult getMessage(final long offset, final int size) {
        //获取文件大小
        int mappedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMapedFileSizeCommitLog();
        //根据消息偏移量查找MappedFile
        MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset, offset == 0);
        if (mappedFile != null) {
            //得到文件内的偏移量
            int pos = (int) (offset % mappedFileSize);
            //从该偏移量读取长度的内容。
            return mappedFile.selectMappedBuffer(pos, size);
        }
        return null;
    }

    /**
     * 根据offset获取下一个文件的偏移量
     * 首先获取文件的大小，减去offset % mappedFileSize，目的是为了回到下一个文件的起始偏移量。
     *
     * 打个比方，正常情况下每个文件的起始偏移量依次是0,1G,2G,3G,4G,5G... 依次类推 mappedFileSize=1G
     * 如果此时Offset= 2G+100，那么下一个文件的起始偏移量就是3G
     * 此时offset % mappedFileSize = 100
     * 那么offset + mappedFileSize - offset % mappedFileSize = 3G.
     */
    public long rollNextFile(final long offset) {
        int mappedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMapedFileSizeCommitLog();
        return offset + mappedFileSize - offset % mappedFileSize;
    }

    public HashMap<String, Long> getTopicQueueTable() {
        return topicQueueTable;
    }

    public void setTopicQueueTable(HashMap<String, Long> topicQueueTable) {
        this.topicQueueTable = topicQueueTable;
    }

    public void destroy() {
        this.mappedFileQueue.destroy();
    }

    public boolean appendData(long startOffset, byte[] data) {
        putMessageLock.lock();
        try {
            MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile(startOffset);
            if (null == mappedFile) {
                log.error("appendData getLastMappedFile error  " + startOffset);
                return false;
            }

            return mappedFile.appendMessage(data);
        } finally {
            putMessageLock.unlock();
        }
    }

    public boolean retryDeleteFirstFile(final long intervalForcibly) {
        return this.mappedFileQueue.retryDeleteFirstFile(intervalForcibly);
    }

    public void removeQueueFromTopicQueueTable(final String topic, final int queueId) {
        String key = topic + "-" + queueId;
        synchronized (this) {
            this.topicQueueTable.remove(key);
        }

        log.info("removeQueueFromTopicQueueTable OK Topic: {} QueueId: {}", topic, queueId);
    }

    public void checkSelf() {
        mappedFileQueue.checkSelf();
    }

    public long lockTimeMills() {
        long diff = 0;
        long begin = this.beginTimeInLock;
        if (begin > 0) {
            diff = this.defaultMessageStore.now() - begin;
        }

        if (diff < 0) {
            diff = 0;
        }

        return diff;
    }

    abstract class FlushCommitLogService extends ServiceThread {
        protected static final int RETRY_TIMES_OVER = 10;
    }

    class CommitRealTimeService extends FlushCommitLogService {

        /**
         * 上一次提交时间
         */
        private long lastCommitTimestamp = 0;

        @Override
        public String getServiceName() {
            return CommitRealTimeService.class.getSimpleName();
        }

        @Override
        public void run() {
            CommitLog.log.info(this.getServiceName() + " service started");
            while (!this.isStopped()) {
                //CommitRealTimeService线程间隔时间，默认200ms
                int interval = CommitLog.this.defaultMessageStore.getMessageStoreConfig().getCommitIntervalCommitLog();
                //异步刷盘时一次提交任务至少包含页数，如果待提交数据不足，小于该参数配置的值，将忽略本次提交，默认4页
                int commitDataLeastPages = CommitLog.this.defaultMessageStore.getMessageStoreConfig().getCommitCommitLogLeastPages();
                //两次真实提交最大间隔，默认200ms
                int commitDataThoroughInterval =
                    CommitLog.this.defaultMessageStore.getMessageStoreConfig().getCommitCommitLogThoroughInterval();

                long begin = System.currentTimeMillis();
                //如果距离上次提交间隔超过commitDataThoroughInterval，则本次提交忽略commitDataLeastPages参数，
                //也就是当commitDataLeastPages=0时，表示如果待提交数据小于指定的页数，也执行提交操作
                if (begin >= (this.lastCommitTimestamp + commitDataThoroughInterval)) {
                    this.lastCommitTimestamp = begin;
                    commitDataLeastPages = 0;
                }

                try {
                    //执行提交操作，将待提交数据提交到物理文件的内存映射区，如果返回false，表示有数据提交成功，唤醒刷盘线程执行刷盘操作。
                    boolean result = CommitLog.this.mappedFileQueue.commit(commitDataLeastPages);
                    long end = System.currentTimeMillis();
                    if (!result) {
                        this.lastCommitTimestamp = end; // result = false means some data committed.
                        //now wake up flush thread.
                        /*
                         * 有数据提交唤醒刷盘线程。
                         * 根据是否开启transientStorePoolEnable机制来唤醒不同的线程
                         * 开启的话唤醒FlushRealTimeService线程
                         * 未开启的话唤醒GroupCommitService线程
                         */
                        flushCommitLogService.wakeup();
                    }

                    if (end - begin > 500) {
                        log.info("Commit data to file costs {} ms", end - begin);
                    }
                    /* 每提交一次，都会尝试休息200ms，
                     * 除非有消息来提前唤醒该线程。也就是方法中
                     * {@link CommitLog#handleDiskFlush(org.apache.rocketmq.store.AppendMessageResult, org.apache.rocketmq.store.PutMessageResult, org.apache.rocketmq.common.message.MessageExt)}
                     * 调用此线程的wakeup方法
                     */
                    this.waitForRunning(interval);
                } catch (Throwable e) {
                    CommitLog.log.error(this.getServiceName() + " service has exception. ", e);
                }
            }

            boolean result = false;
            for (int i = 0; i < RETRY_TIMES_OVER && !result; i++) {
                result = CommitLog.this.mappedFileQueue.commit(0);
                CommitLog.log.info(this.getServiceName() + " service shutdown, retry " + (i + 1) + " times " + (result ? "OK" : "Not OK"));
            }
            CommitLog.log.info(this.getServiceName() + " service end");
        }
    }

    class FlushRealTimeService extends FlushCommitLogService {
        /**
         * 上一次刷盘时间
         */
        private long lastFlushTimestamp = 0;
        private long printTimes = 0;

        public void run() {
            CommitLog.log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                //默认为false，标识await方法等待时间；如果为true，标识使用Thread.sleep方法等待
                boolean flushCommitLogTimed = CommitLog.this.defaultMessageStore.getMessageStoreConfig().isFlushCommitLogTimed();

                //FlushRealTimeService线程运行任务间隔，默认50ms
                int interval = CommitLog.this.defaultMessageStore.getMessageStoreConfig().getFlushIntervalCommitLog();
                // 一次刷盘任务至少包含页数，如果待刷写数据不足，小于该参数配置的值，将忽略本息刷盘任务，默认4页
                int flushPhysicQueueLeastPages = CommitLog.this.defaultMessageStore.getMessageStoreConfig().getFlushCommitLogLeastPages();

                //两次刷写任务最大时间间隔，默认10s
                int flushPhysicQueueThoroughInterval =
                    CommitLog.this.defaultMessageStore.getMessageStoreConfig().getFlushCommitLogThoroughInterval();

                boolean printFlushProgress = false;

                // Print flush progress
                long currentTimeMillis = System.currentTimeMillis();
                //如果距上次刷盘时间超过flushPhysicQueueThoroughInterval，则本次刷盘忽略flushPhysicQueueLeastPages参数，
                //就是当flushPhysicQueueLeastPages=0时，表示如果待刷盘数据小于指定的页数，也执行刷盘操作
                if (currentTimeMillis >= (this.lastFlushTimestamp + flushPhysicQueueThoroughInterval)) {
                    this.lastFlushTimestamp = currentTimeMillis;
                    flushPhysicQueueLeastPages = 0;
                    printFlushProgress = (printTimes++ % 10) == 0;
                }

                try {
                    //执行一次刷盘任务时先等待指定的时间，然后再执行刷盘任务
                    if (flushCommitLogTimed) {
                        Thread.sleep(interval);
                    } else {
                        this.waitForRunning(interval);
                    }

                    //打印刷盘进度，目前空函数
                    if (printFlushProgress) {
                        this.printFlushProgress();
                    }

                    long begin = System.currentTimeMillis();
                    /**
                     * 刷盘，并且更新检测点文件的commitlog文件的更新时间戳，文件检测点的刷盘动作在刷盘消息消费队列线程中执行，
                     * 其入口为{@link DefaultMessageStore.FlushConsumeQueueService#doFlush(int)}
                     * 或者
                     * {@link org.apache.rocketmq.store.index.IndexService#flush(org.apache.rocketmq.store.index.IndexFile)}
                     */
                    CommitLog.this.mappedFileQueue.flush(flushPhysicQueueLeastPages);
                    long storeTimestamp = CommitLog.this.mappedFileQueue.getStoreTimestamp();
                    if (storeTimestamp > 0) {
                        CommitLog.this.defaultMessageStore.getStoreCheckpoint().setPhysicMsgTimestamp(storeTimestamp);
                    }
                    long past = System.currentTimeMillis() - begin;
                    if (past > 500) {
                        log.info("Flush data to disk costs {} ms", past);
                    }
                } catch (Throwable e) {
                    CommitLog.log.warn(this.getServiceName() + " service has exception. ", e);
                    this.printFlushProgress();
                }
            }

            // Normal shutdown, to ensure that all the flush before exit
            boolean result = false;
            for (int i = 0; i < RETRY_TIMES_OVER && !result; i++) {
                result = CommitLog.this.mappedFileQueue.flush(0);
                CommitLog.log.info(this.getServiceName() + " service shutdown, retry " + (i + 1) + " times " + (result ? "OK" : "Not OK"));
            }

            this.printFlushProgress();

            CommitLog.log.info(this.getServiceName() + " service end");
        }

        @Override
        public String getServiceName() {
            return FlushRealTimeService.class.getSimpleName();
        }

        private void printFlushProgress() {
            // CommitLog.log.info("how much disk fall behind memory, "
            // + CommitLog.this.mappedFileQueue.howMuchFallBehind());
        }

        @Override
        public long getJointime() {
            return 1000 * 60 * 5;
        }
    }

    public static class GroupCommitRequest {

        /**
         * 刷盘点偏移量
         * 本条消息在commitLog中的最大物理偏移量-1，即当前需要将nextOffset指针前的消息刷盘
         */
        private final long nextOffset;

        /**
         * 倒计数锁存器
         */
        private final CountDownLatch countDownLatch = new CountDownLatch(1);

        /**
         * 刷盘结果，初始为false
         */
        private volatile boolean flushOK = false;

        public GroupCommitRequest(long nextOffset) {
            this.nextOffset = nextOffset;
        }

        public long getNextOffset() {
            return nextOffset;
        }

        /**
         * GroupCommitService线程处理GroupCommitRequest对象后将调用wakeupCustomer方法将消费发送线程唤醒，
         * 并且将刷盘结果告知GroupCommitRequest。
         *
         */
        public void wakeupCustomer(final boolean flushOK) {
            this.flushOK = flushOK;
            this.countDownLatch.countDown();
        }

        /**
         * 消费发送线程将消息追加到内存映射文件后，将同步任务GroupCommitRequest提交到GroupCommitService线程，
         * 然后调用阻塞线程等待刷盘结果，默认超时时间为5s。
         * @param timeout
         * @return
         */
        public boolean waitForFlush(long timeout) {
            try {
                this.countDownLatch.await(timeout, TimeUnit.MILLISECONDS);
                return this.flushOK;
            } catch (InterruptedException e) {
                log.error("Interrupted", e);
                return false;
            }
        }
    }

    /**
     * GroupCommit Service
     */
    class GroupCommitService extends FlushCommitLogService {
        /**
         * 同步刷盘暂存容器
         */
        private volatile List<GroupCommitRequest> requestsWrite = new ArrayList<GroupCommitRequest>();
        /**
         * GroupCommitService线程每次处理的request容器
         * 这是一个设计亮点，避免了任务提交与任务执行的锁冲突。
         *
         * 在提交刷盘任务时将任务提交至requestsWrite中
         * 而当前线程中的执行的刷盘任务是requestsWrite中的，执行完后将requestsWrite清除，
         * 再交换requestsWrite与requestsRead中的值
         */
        private volatile List<GroupCommitRequest> requestsRead = new ArrayList<GroupCommitRequest>();

        /**
         * 客户端提交同步刷盘任务到GroupCommitService线程，如果线程处于等待状态，则将其唤醒。
         */
        public synchronized void putRequest(final GroupCommitRequest request) {
            synchronized (this.requestsWrite) {
                this.requestsWrite.add(request);
            }
            //线程处于等待状态，则将其唤醒。
            if (hasNotified.compareAndSet(false, true)) {
                waitPoint.countDown(); // notify
            }
        }

        /**
         * 由于避免同步刷盘消费任务与其他消息产生的任务直接的锁竞争，
         * GroupCommitService提供读容器与写容器，这两个容器每执行完一次任务后，交互，继续消费任务。
         */
        private void swapRequests() {
            List<GroupCommitRequest> tmp = this.requestsWrite;
            this.requestsWrite = this.requestsRead;
            this.requestsRead = tmp;
        }

        private void doCommit() {
            synchronized (this.requestsRead) {
                if (!this.requestsRead.isEmpty()) {
                    //比那里同步刷盘任务列表，根据加入顺序逐一执行刷盘逻辑
                    for (GroupCommitRequest req : this.requestsRead) {
                        // There may be a message in the next file, so a maximum of
                        // two times the flush
                        boolean flushOK = false;
                        for (int i = 0; i < 2 && !flushOK; i++) {

                            /*
                             * 1. 正常情况下第一次循环进来，本次需要刷盘指针处的消息（req.getNextOffset()）肯定还未刷盘，所以flushOK==false，
                             *    然后接着下面的刷盘，接着进入第二次循环
                             * 2. 第二次循环到这里校验下本次本次需要刷盘指针处的消息（req.getNextOffset()）是否刷盘，如果flushOK==true那么标识已经刷盘，否则未刷盘成功。
                             *
                             */
                            flushOK = CommitLog.this.mappedFileQueue.getFlushedWhere() >= req.getNextOffset();

                            //当前消息没有刷盘则刷盘
                            if (!flushOK) {
                                //刷盘
                                /**
                                 * 同步刷盘策略，如果开启transientStorePoolEnable机制时为什么这里也是直接刷盘，而不用先提交数据呢?
                                 * 因为开启transientStorePoolEnable机制，会有{@link org.apache.rocketmq.store.CommitLog.CommitRealTimeService#run()}
                                 * 线程默认每200ms执行一次提交操作，因此这里直接输盘即可。
                                 */
                                CommitLog.this.mappedFileQueue.flush(0);
                            }
                        }

                        // 唤醒消息发送线程并同时刷盘结果
                        req.wakeupCustomer(flushOK);
                    }

                    /**
                     * 更新消息的刷盘时间，但是并没有执行刷盘操作，检测刷盘操作将在写消息队列的文件时触发。
                     * 其入口为{@link DefaultMessageStore.FlushConsumeQueueService#doFlush(int)}
                     */
                    long storeTimestamp = CommitLog.this.mappedFileQueue.getStoreTimestamp();
                    if (storeTimestamp > 0) {
                        CommitLog.this.defaultMessageStore.getStoreCheckpoint().setPhysicMsgTimestamp(storeTimestamp);
                    }

                    //清除容器内容
                    this.requestsRead.clear();
                } else {
                    // Because of individual messages is set to not sync flush, it
                    // will come to this process
                    CommitLog.this.mappedFileQueue.flush(0);
                }
            }
        }

        /**
         * GroupCommitService组提交线程，没处理一批刷盘请求后，如果后续有待刷盘的请求需要处理时，
         * 组提交线程会马不停蹄对的处理下一批，如果没有待处理的任务，则休息10ms，即每10ms空转一次。
         */
        public void run() {
            CommitLog.log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    this.waitForRunning(10);
                    this.doCommit();
                } catch (Exception e) {
                    CommitLog.log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            // Under normal circumstances shutdown, wait for the arrival of the
            // request, and then flush
            // 正常关闭的情况下，等到20毫秒后，继续刷盘
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                CommitLog.log.warn("GroupCommitService Exception, ", e);
            }

            synchronized (this) {
                this.swapRequests();
            }

            this.doCommit();

            CommitLog.log.info(this.getServiceName() + " service end");
        }

        @Override
        protected void onWaitEnd() {
            this.swapRequests();
        }

        @Override
        public String getServiceName() {
            return GroupCommitService.class.getSimpleName();
        }

        @Override
        public long getJointime() {
            return 1000 * 60 * 5;
        }
    }

    class DefaultAppendMessageCallback implements AppendMessageCallback {
        // File at the end of the minimum fixed length empty
        /**
         * 标识文件末尾至少有8个字节为空
         */
        private static final int END_FILE_MIN_BLANK_LENGTH = 4 + 4;
        private final ByteBuffer msgIdMemory;
        // Store the message content
        private final ByteBuffer msgStoreItemMemory;
        // The maximum length of the message
        /**
         * 每个消息条目的最大大小，默认4M
         * @see MessageStoreConfig#maxMessageSize
         */
        private final int maxMessageSize;
        // Build Message Key
        private final StringBuilder keyBuilder = new StringBuilder();

        private final StringBuilder msgIdBuilder = new StringBuilder();

        private final ByteBuffer hostHolder = ByteBuffer.allocate(8);

        DefaultAppendMessageCallback(final int size) {
            this.msgIdMemory = ByteBuffer.allocate(MessageDecoder.MSG_ID_LENGTH);
            this.msgStoreItemMemory = ByteBuffer.allocate(size + END_FILE_MIN_BLANK_LENGTH);
            this.maxMessageSize = size;
        }

        public ByteBuffer getMsgStoreItemMemory() {
            return msgStoreItemMemory;
        }

        public AppendMessageResult doAppend(final long fileFromOffset, final ByteBuffer byteBuffer, final int maxBlank,
            final MessageExtBrokerInner msgInner) {
            // STORETIMESTAMP + STOREHOSTADDRESS + OFFSET <br>

            // PHY OFFSET
            // 计算当前写的物理偏移量
            long wroteOffset = fileFromOffset + byteBuffer.position();

            this.resetByteBuffer(hostHolder, 8);
            //创建全局唯一消息id，此消息id有16个字节。4字节IP，4字节端口号，8字节消息偏移量
            // 根据消息ID可以获取消息偏移量可以快速找到消息内容。
            String msgId = MessageDecoder.createMessageId(this.msgIdMemory, msgInner.getStoreHostBytes(hostHolder), wroteOffset);

            // Record ConsumeQueue information
            keyBuilder.setLength(0);
            keyBuilder.append(msgInner.getTopic());
            keyBuilder.append('-');
            keyBuilder.append(msgInner.getQueueId());
            String key = keyBuilder.toString();
            //获取该消息在消息消费队列中的index（ConsumeQueue），topicQueueTable中保存当前所有消息队列的当前写入偏移量。
            Long queueOffset = CommitLog.this.topicQueueTable.get(key);
            if (null == queueOffset) {
                //为空说明是此topic-queueId的第一条消息，初始化为0
                queueOffset = 0L;
                CommitLog.this.topicQueueTable.put(key, queueOffset);
            }

            // Transaction messages that require special handling
            final int tranType = MessageSysFlag.getTransactionValue(msgInner.getSysFlag());
            switch (tranType) {
                // Prepared and Rollback message is not consumed, will not enter the
                // consumer queuec
                case MessageSysFlag.TRANSACTION_PREPARED_TYPE:
                case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE:
                    queueOffset = 0L;
                    break;
                case MessageSysFlag.TRANSACTION_NOT_TYPE:
                case MessageSysFlag.TRANSACTION_COMMIT_TYPE:
                default:
                    break;
            }

            /**
             * Serialize message
             */
            //获取消息扩展属性
            final byte[] propertiesData =
                msgInner.getPropertiesString() == null ? null : msgInner.getPropertiesString().getBytes(MessageDecoder.CHARSET_UTF8);

            final int propertiesLength = propertiesData == null ? 0 : propertiesData.length;

            if (propertiesLength > Short.MAX_VALUE) {
                log.warn("putMessage message properties length too long. length={}", propertiesData.length);
                return new AppendMessageResult(AppendMessageStatus.PROPERTIES_SIZE_EXCEEDED);
            }

            //获取消息主题
            final byte[] topicData = msgInner.getTopic().getBytes(MessageDecoder.CHARSET_UTF8);
            final int topicLength = topicData.length;

            //获取消息体
            final int bodyLength = msgInner.getBody() == null ? 0 : msgInner.getBody().length;

            //根据消息体长度、主题长度、属性的长度结合存储格式计算消息的总长度。
            final int msgLen = calMsgLength(bodyLength, topicLength, propertiesLength);

            // Exceeds the maximum message
            if (msgLen > this.maxMessageSize) {
                CommitLog.log.warn("message size exceeded, msg total size: " + msgLen + ", msg body size: " + bodyLength
                    + ", maxMessageSize: " + this.maxMessageSize);
                return new AppendMessageResult(AppendMessageStatus.MESSAGE_SIZE_EXCEEDED);
            }

            // Determines whether there is sufficient free space
            /**
             * 判断当前文件中是否有足够的空间存储此消息
             * 如果没有足够的空间
             * 那么返回{@link AppendMessageStatus.END_OF_FILE}，Broker会重新创建一个新的CommitLog来存储该消息。
             * 从这里可以看出每个CommitLog文件至少会空闲8个字节，
             * 高4字节存储当前文件剩余空间，低4细节存储魔数{@link CommitLog.BLANK_MAGIC_CODE}
             */
            if ((msgLen + END_FILE_MIN_BLANK_LENGTH) > maxBlank) {
                this.resetByteBuffer(this.msgStoreItemMemory, maxBlank);
                // 1 TOTALSIZE
                this.msgStoreItemMemory.putInt(maxBlank);
                // 2 MAGICCODE
                this.msgStoreItemMemory.putInt(CommitLog.BLANK_MAGIC_CODE);
                // 3 The remaining space may be any value
                // Here the length of the specially set maxBlank
                final long beginTimeMills = CommitLog.this.defaultMessageStore.now();
                byteBuffer.put(this.msgStoreItemMemory.array(), 0, maxBlank);
                return new AppendMessageResult(AppendMessageStatus.END_OF_FILE, wroteOffset, maxBlank, msgId, msgInner.getStoreTimestamp(),
                    queueOffset, CommitLog.this.defaultMessageStore.now() - beginTimeMills);
            }

            // Initialization of storage space
            this.resetByteBuffer(msgStoreItemMemory, msgLen);
            // 1 TOTALSIZE 该消息总长度四个字节
            this.msgStoreItemMemory.putInt(msgLen);
            // 2 MAGICCODE 魔术四个字节。固定值 0xdaa3207a
            this.msgStoreItemMemory.putInt(CommitLog.MESSAGE_MAGIC_CODE);
            // 3 BODYCRC 消息体crc检验码，4个字节
            this.msgStoreItemMemory.putInt(msgInner.getBodyCRC());
            // 4 QUEUEID 消息消费队列，4字节
            this.msgStoreItemMemory.putInt(msgInner.getQueueId());
            // 5 FLAG 消息Flag，RocketMQ不处理，供程序使用，默认4字节
            this.msgStoreItemMemory.putInt(msgInner.getFlag());
            // 6 QUEUEOFFSET 消息在消息消费队列的偏移量 8字节
            this.msgStoreItemMemory.putLong(queueOffset);
            // 7 PHYSICALOFFSET 消息在CommitLog文件中的偏移量，8字节
            this.msgStoreItemMemory.putLong(fileFromOffset + byteBuffer.position());
            // 8 SYSFLAG 消息系统FLAG，例如是否压缩、是否是事务消息等，4字节
            this.msgStoreItemMemory.putInt(msgInner.getSysFlag());
            // 9 BORNTIMESTAMP 消息生产者调用消息发送API的时间戳，8字节
            this.msgStoreItemMemory.putLong(msgInner.getBornTimestamp());
            // 10 BORNHOST 消息发送者ip+端口，8字节
            this.resetByteBuffer(hostHolder, 8);
            this.msgStoreItemMemory.put(msgInner.getBornHostBytes(hostHolder));
            // 11 STORETIMESTAMP 消息存储时间戳，8字节
            this.msgStoreItemMemory.putLong(msgInner.getStoreTimestamp());
            // 12 STOREHOSTADDRESS Broker服务器ip+端口，8字节
            this.resetByteBuffer(hostHolder, 8);
            this.msgStoreItemMemory.put(msgInner.getStoreHostBytes(hostHolder));
            //this.msgBatchMemory.put(msgInner.getStoreHostBytes());
            // 13 RECONSUMETIMES 消息重试次数，4字节
            this.msgStoreItemMemory.putInt(msgInner.getReconsumeTimes());
            // 14 Prepared Transaction Offset 事务消息物理偏移量，8字节
            this.msgStoreItemMemory.putLong(msgInner.getPreparedTransactionOffset());
            // 15 BODY 消息体长度 4字节
            this.msgStoreItemMemory.putInt(bodyLength);
            if (bodyLength > 0)
                //消息体内容，长度为bodyLength
                this.msgStoreItemMemory.put(msgInner.getBody());
            // 16 TOPIC
            //主题存储长度 1字节，标识主题名称不能超过255字节
            this.msgStoreItemMemory.put((byte) topicLength);
            //主题，长度为topicLength
            this.msgStoreItemMemory.put(topicData);
            // 17 PROPERTIES 消息属性长度，2字节，标识消息属性不能超过65536个字符
            this.msgStoreItemMemory.putShort((short) propertiesLength);
            if (propertiesLength > 0)
                //消息属性，长度为propertiesLength
                this.msgStoreItemMemory.put(propertiesData);

            final long beginTimeMills = CommitLog.this.defaultMessageStore.now();
            // Write messages to the queue buffer
            //将消息存储在byteBuffer中，这里没有刷写到磁盘。
            byteBuffer.put(this.msgStoreItemMemory.array(), 0, msgLen);

            AppendMessageResult result = new AppendMessageResult(AppendMessageStatus.PUT_OK, wroteOffset, msgLen, msgId,
                msgInner.getStoreTimestamp(), queueOffset, CommitLog.this.defaultMessageStore.now() - beginTimeMills);

            //更新消息队列逻辑偏移量
            switch (tranType) {
                case MessageSysFlag.TRANSACTION_PREPARED_TYPE:
                case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE:
                    break;
                case MessageSysFlag.TRANSACTION_NOT_TYPE:
                case MessageSysFlag.TRANSACTION_COMMIT_TYPE:
                    // The next update ConsumeQueue information
                    //设置下一个消息的在消息消费队列中的index（下标）
                    CommitLog.this.topicQueueTable.put(key, ++queueOffset);
                    break;
                default:
                    break;
            }
            return result;
        }

        public AppendMessageResult doAppend(final long fileFromOffset, final ByteBuffer byteBuffer, final int maxBlank,
            final MessageExtBatch messageExtBatch) {
            byteBuffer.mark();
            //physical offset
            long wroteOffset = fileFromOffset + byteBuffer.position();
            // Record ConsumeQueue information
            keyBuilder.setLength(0);
            keyBuilder.append(messageExtBatch.getTopic());
            keyBuilder.append('-');
            keyBuilder.append(messageExtBatch.getQueueId());
            String key = keyBuilder.toString();
            Long queueOffset = CommitLog.this.topicQueueTable.get(key);
            if (null == queueOffset) {
                queueOffset = 0L;
                CommitLog.this.topicQueueTable.put(key, queueOffset);
            }
            long beginQueueOffset = queueOffset;
            int totalMsgLen = 0;
            int msgNum = 0;
            msgIdBuilder.setLength(0);
            final long beginTimeMills = CommitLog.this.defaultMessageStore.now();
            ByteBuffer messagesByteBuff = messageExtBatch.getEncodedBuff();
            this.resetByteBuffer(hostHolder, 8);
            ByteBuffer storeHostBytes = messageExtBatch.getStoreHostBytes(hostHolder);
            messagesByteBuff.mark();
            while (messagesByteBuff.hasRemaining()) {
                // 1 TOTALSIZE
                final int msgPos = messagesByteBuff.position();
                final int msgLen = messagesByteBuff.getInt();
                final int bodyLen = msgLen - 40; //only for log, just estimate it
                // Exceeds the maximum message
                if (msgLen > this.maxMessageSize) {
                    CommitLog.log.warn("message size exceeded, msg total size: " + msgLen + ", msg body size: " + bodyLen
                        + ", maxMessageSize: " + this.maxMessageSize);
                    return new AppendMessageResult(AppendMessageStatus.MESSAGE_SIZE_EXCEEDED);
                }
                totalMsgLen += msgLen;
                // Determines whether there is sufficient free space
                if ((totalMsgLen + END_FILE_MIN_BLANK_LENGTH) > maxBlank) {
                    this.resetByteBuffer(this.msgStoreItemMemory, 8);
                    // 1 TOTALSIZE
                    this.msgStoreItemMemory.putInt(maxBlank);
                    // 2 MAGICCODE
                    this.msgStoreItemMemory.putInt(CommitLog.BLANK_MAGIC_CODE);
                    // 3 The remaining space may be any value
                    //ignore previous read
                    messagesByteBuff.reset();
                    // Here the length of the specially set maxBlank
                    byteBuffer.reset(); //ignore the previous appended messages
                    byteBuffer.put(this.msgStoreItemMemory.array(), 0, 8);
                    return new AppendMessageResult(AppendMessageStatus.END_OF_FILE, wroteOffset, maxBlank, msgIdBuilder.toString(), messageExtBatch.getStoreTimestamp(),
                        beginQueueOffset, CommitLog.this.defaultMessageStore.now() - beginTimeMills);
                }
                //move to add queue offset and commitlog offset
                messagesByteBuff.position(msgPos + 20);
                messagesByteBuff.putLong(queueOffset);
                messagesByteBuff.putLong(wroteOffset + totalMsgLen - msgLen);

                storeHostBytes.rewind();
                String msgId = MessageDecoder.createMessageId(this.msgIdMemory, storeHostBytes, wroteOffset + totalMsgLen - msgLen);
                if (msgIdBuilder.length() > 0) {
                    msgIdBuilder.append(',').append(msgId);
                } else {
                    msgIdBuilder.append(msgId);
                }
                queueOffset++;
                msgNum++;
                messagesByteBuff.position(msgPos + msgLen);
            }

            messagesByteBuff.position(0);
            messagesByteBuff.limit(totalMsgLen);
            byteBuffer.put(messagesByteBuff);
            messageExtBatch.setEncodedBuff(null);
            AppendMessageResult result = new AppendMessageResult(AppendMessageStatus.PUT_OK, wroteOffset, totalMsgLen, msgIdBuilder.toString(),
                messageExtBatch.getStoreTimestamp(), beginQueueOffset, CommitLog.this.defaultMessageStore.now() - beginTimeMills);
            result.setMsgNum(msgNum);
            CommitLog.this.topicQueueTable.put(key, queueOffset);

            return result;
        }

        private void resetByteBuffer(final ByteBuffer byteBuffer, final int limit) {
            byteBuffer.flip();
            byteBuffer.limit(limit);
        }

    }

    public static class MessageExtBatchEncoder {
        // Store the message content
        private final ByteBuffer msgBatchMemory;
        // The maximum length of the message
        private final int maxMessageSize;

        private final ByteBuffer hostHolder = ByteBuffer.allocate(8);

        MessageExtBatchEncoder(final int size) {
            this.msgBatchMemory = ByteBuffer.allocateDirect(size);
            this.maxMessageSize = size;
        }

        public ByteBuffer encode(final MessageExtBatch messageExtBatch) {
            msgBatchMemory.clear(); //not thread-safe
            int totalMsgLen = 0;
            ByteBuffer messagesByteBuff = messageExtBatch.wrap();
            while (messagesByteBuff.hasRemaining()) {
                // 1 TOTALSIZE
                messagesByteBuff.getInt();
                // 2 MAGICCODE
                messagesByteBuff.getInt();
                // 3 BODYCRC
                messagesByteBuff.getInt();
                // 4 FLAG
                int flag = messagesByteBuff.getInt();
                // 5 BODY
                int bodyLen = messagesByteBuff.getInt();
                int bodyPos = messagesByteBuff.position();
                int bodyCrc = UtilAll.crc32(messagesByteBuff.array(), bodyPos, bodyLen);
                messagesByteBuff.position(bodyPos + bodyLen);
                // 6 properties
                short propertiesLen = messagesByteBuff.getShort();
                int propertiesPos = messagesByteBuff.position();
                messagesByteBuff.position(propertiesPos + propertiesLen);

                final byte[] topicData = messageExtBatch.getTopic().getBytes(MessageDecoder.CHARSET_UTF8);

                final int topicLength = topicData.length;

                final int msgLen = calMsgLength(bodyLen, topicLength, propertiesLen);

                // Exceeds the maximum message
                if (msgLen > this.maxMessageSize) {
                    CommitLog.log.warn("message size exceeded, msg total size: " + msgLen + ", msg body size: " + bodyLen
                        + ", maxMessageSize: " + this.maxMessageSize);
                    throw new RuntimeException("message size exceeded");
                }

                totalMsgLen += msgLen;
                // Determines whether there is sufficient free space
                if (totalMsgLen > maxMessageSize) {
                    throw new RuntimeException("message size exceeded");
                }

                // 1 TOTALSIZE
                this.msgBatchMemory.putInt(msgLen);
                // 2 MAGICCODE
                this.msgBatchMemory.putInt(CommitLog.MESSAGE_MAGIC_CODE);
                // 3 BODYCRC
                this.msgBatchMemory.putInt(bodyCrc);
                // 4 QUEUEID
                this.msgBatchMemory.putInt(messageExtBatch.getQueueId());
                // 5 FLAG
                this.msgBatchMemory.putInt(flag);
                // 6 QUEUEOFFSET
                this.msgBatchMemory.putLong(0);
                // 7 PHYSICALOFFSET
                this.msgBatchMemory.putLong(0);
                // 8 SYSFLAG
                this.msgBatchMemory.putInt(messageExtBatch.getSysFlag());
                // 9 BORNTIMESTAMP
                this.msgBatchMemory.putLong(messageExtBatch.getBornTimestamp());
                // 10 BORNHOST
                this.resetByteBuffer(hostHolder, 8);
                this.msgBatchMemory.put(messageExtBatch.getBornHostBytes(hostHolder));
                // 11 STORETIMESTAMP
                this.msgBatchMemory.putLong(messageExtBatch.getStoreTimestamp());
                // 12 STOREHOSTADDRESS
                this.resetByteBuffer(hostHolder, 8);
                this.msgBatchMemory.put(messageExtBatch.getStoreHostBytes(hostHolder));
                // 13 RECONSUMETIMES
                this.msgBatchMemory.putInt(messageExtBatch.getReconsumeTimes());
                // 14 Prepared Transaction Offset, batch does not support transaction
                this.msgBatchMemory.putLong(0);
                // 15 BODY
                this.msgBatchMemory.putInt(bodyLen);
                if (bodyLen > 0)
                    this.msgBatchMemory.put(messagesByteBuff.array(), bodyPos, bodyLen);
                // 16 TOPIC
                this.msgBatchMemory.put((byte) topicLength);
                this.msgBatchMemory.put(topicData);
                // 17 PROPERTIES
                this.msgBatchMemory.putShort(propertiesLen);
                if (propertiesLen > 0)
                    this.msgBatchMemory.put(messagesByteBuff.array(), propertiesPos, propertiesLen);
            }
            msgBatchMemory.flip();
            return msgBatchMemory;
        }

        private void resetByteBuffer(final ByteBuffer byteBuffer, final int limit) {
            byteBuffer.flip();
            byteBuffer.limit(limit);
        }

    }
}

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
package org.apache.rocketmq.broker.transaction.queue;

import org.apache.rocketmq.broker.transaction.AbstractTransactionalMessageCheckListener;
import org.apache.rocketmq.broker.transaction.OperationResult;
import org.apache.rocketmq.broker.transaction.TransactionalMessageService;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.EndTransactionRequestHeader;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class TransactionalMessageServiceImpl implements TransactionalMessageService {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.TRANSACTION_LOGGER_NAME);

    private TransactionalMessageBridge transactionalMessageBridge;

    private static final int PULL_MSG_RETRY_NUMBER = 1;

    private static final int MAX_PROCESS_TIME_LIMIT = 60000;

    private static final int MAX_RETRY_COUNT_WHEN_HALF_NULL = 1;

    public TransactionalMessageServiceImpl(TransactionalMessageBridge transactionBridge) {
        this.transactionalMessageBridge = transactionBridge;
    }

    /**
     * key 是事务prepare消息对应的MessageQueue，其topic是{@link MixAll#RMQ_SYS_TRANS_HALF_TOPIC}
     *
     * value 是事务prepare消息提交或者回滚时，将prepare消息存储到commitlog中对应的key
     * 其topic是{@link MixAll#RMQ_SYS_TRANS_OP_HALF_TOPIC}
     *
     * 两个MessageQueue除了topic不一致外，其他的属性都相等
     *
     */
    private ConcurrentHashMap<MessageQueue, MessageQueue> opQueueMap = new ConcurrentHashMap<>();

    @Override
    public PutMessageResult prepareMessage(MessageExtBrokerInner messageInner) {
        return transactionalMessageBridge.putHalfMessage(messageInner);
    }

    private boolean needDiscard(MessageExt msgExt, int transactionCheckMax) {
        //获取消息回查次数
        String checkTimes = msgExt.getProperty(MessageConst.PROPERTY_TRANSACTION_CHECK_TIMES);
        int checkTime = 1;
        if (null != checkTimes) {
            checkTime = getInt(checkTimes);
            //如果消息回查次数大于允许回查的最大次数，即事务提交失败
            if (checkTime >= transactionCheckMax) {
                return true;
            } else {
                checkTime++;
            }
        }
        //每回查一次+1
        msgExt.putUserProperty(MessageConst.PROPERTY_TRANSACTION_CHECK_TIMES, String.valueOf(checkTime));
        return false;
    }

    private boolean needSkip(MessageExt msgExt) {
        long valueOfCurrentMinusBorn = System.currentTimeMillis() - msgExt.getBornTimestamp();
        //如果事务消息超过文件的过期时间，默认72小时，跳过该消息。
        if (valueOfCurrentMinusBorn
            > transactionalMessageBridge.getBrokerController().getMessageStoreConfig().getFileReservedTime()
            * 3600L * 1000) {
            log.info("Half message exceed file reserved time ,so skip it.messageId {},bornTime {}",
                msgExt.getMsgId(), msgExt.getBornTimestamp());
            return true;
        }
        return false;
    }

    /**
     * 在执行事务消息回查之前，会把该消息再次存储到commitlog找那个，
     * 当前消息设置最新的物理偏移量。为什么呢？
     * 只要是因为下文的发送事务消息回查命令是异步处理的，无法同步返回其处理结果，为了避免简化prepare消息队列和处理队列的
     * 消息消费进度，先存储，然后消费进度向前推进，重复发送的事务消息在事务回查之前会判断消息是否处理过。
     * 另外一个目的就是因为需要修改消息的检查次数，RocketMq的存储设计采用的是顺序写，如果去修改已存储的消息，
     * 其性能无法得到保障。
     */
    private boolean putBackHalfMsgQueue(MessageExt msgExt, long offset) {
        PutMessageResult putMessageResult = putBackToHalfQueueReturnResult(msgExt);
        if (putMessageResult != null
            && putMessageResult.getPutMessageStatus() == PutMessageStatus.PUT_OK) {
            msgExt.setQueueOffset(
                putMessageResult.getAppendMessageResult().getLogicsOffset());
            msgExt.setCommitLogOffset(
                putMessageResult.getAppendMessageResult().getWroteOffset());
            msgExt.setMsgId(putMessageResult.getAppendMessageResult().getMsgId());
            log.info(
                "Send check message, the offset={} restored in queueOffset={} "
                    + "commitLogOffset={} "
                    + "newMsgId={} realMsgId={} topic={}",
                offset, msgExt.getQueueOffset(), msgExt.getCommitLogOffset(), msgExt.getMsgId(),
                msgExt.getUserProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX),
                msgExt.getTopic());
            return true;
        } else {
            log.error(
                "PutBackToHalfQueueReturnResult write failed, topic: {}, queueId: {}, "
                    + "msgId: {}",
                msgExt.getTopic(), msgExt.getQueueId(), msgExt.getMsgId());
            return false;
        }
    }

    /**
     *
     * @param transactionTimeout The minimum time of the transactional message to be checked firstly, one message only
     * exceed this time interval that can be checked.
     * 事务过期时间，默认3s
     * 只有当消息存储时间 + transactionTimeOut 大于系统当前时间，
     * 才对消息执行事务状态回查，否则在下一周期中执行事务回查操作
     * @param transactionCheckMax The maximum number of times the message was checked, if exceed this value, this
     * message will be discarded.
     * 事务消息回查最大检测次数，默认5次
     * 如果超过最大检测次数还是无法获知消息的事务状态，将不会继续对消息进行事务回查，而是直接丢弃，相当于事务回滚。
     */
    @Override
    public void check(long transactionTimeout, int transactionCheckMax,
        AbstractTransactionalMessageCheckListener listener) {
        try {
            String topic = MixAll.RMQ_SYS_TRANS_HALF_TOPIC;
            Set<MessageQueue> msgQueues = transactionalMessageBridge.fetchMessageQueues(topic);
            if (msgQueues == null || msgQueues.size() == 0) {
                log.warn("The queue of topic is empty :" + topic);
                return;
            }
            log.info("Check topic={}, queues={}", topic, msgQueues);
            for (MessageQueue messageQueue : msgQueues) {
                long startTime = System.currentTimeMillis();
                //根据事务prepare消息的消息队列获取对应的事务完成后（提交或者换回滚）对应的消息队列
                MessageQueue opQueue = getOpQueue(messageQueue);
                //获取消费队列对应的消息消费进度，返回-1说明消息消费队列中无消息。
                long halfOffset = transactionalMessageBridge.fetchConsumeOffset(messageQueue);
                long opOffset = transactionalMessageBridge.fetchConsumeOffset(opQueue);
                log.info("Before check, the queue={} msgOffset={} opOffset={}", messageQueue, halfOffset, opOffset);
                if (halfOffset < 0 || opOffset < 0) {
                    log.error("MessageQueue: {} illegal offset read: {}, op offset: {},skip this queue", messageQueue,
                        halfOffset, opOffset);
                    continue;
                }

                /**
                 * 事务完成队列（OP队列中）已经处理的消息的偏移量
                 * 需要根据doneOpOffset，更新OP队列消息消费进度
                 */
                List<Long> doneOpOffset = new ArrayList<>();
                /**
                 * 事务prepare消息队列中的消息已经完成，被提交后或者回滚，需要跳过当前偏移量的消息
                 * key 事务prepare消息队列中的offset
                 * value 事务完成消息队列中的offset
                 */
                HashMap<Long, Long> removeMap = new HashMap<>();
                //根据当前的处理进度，从已处理队列（事务完成队列）拉取32条消息，方便判断当前处理的消息（事务prepare消息）是否已经处理过，
                //如果处理过则无需再次发送事务状态回查请求，避免重复发送事务回查请求。
                PullResult pullResult = fillOpRemoveMap(removeMap, opQueue, opOffset, halfOffset, doneOpOffset);
                if (null == pullResult) {
                    log.error("The queue={} check msgOffset={} with opOffset={} failed, pullResult is null",
                        messageQueue, halfOffset, opOffset);
                    continue;
                }
                // single thread
                //获取空消息的次数
                int getMessageNullCount = 1;
                //当前已处理messageQueue的队列偏移量，事务prepare消息队列
                long newOffset = halfOffset;
                //当前正在处理的messageQueue队列偏移量，事务prepare消息队列
                long i = halfOffset;
                while (true) {
                    /**
                     * RocketMq处理一个任务的通用逻辑就是未每个任务一次只分配固定时长，超过该时长则需要下次调度。
                     * 时长为60s，目前不可配置。
                     */
                    if (System.currentTimeMillis() - startTime > MAX_PROCESS_TIME_LIMIT) {
                        log.info("Queue={} process time reach max={}", messageQueue, MAX_PROCESS_TIME_LIMIT);
                        break;
                    }
                    //如果当前消息已被处理，则继续处理下一条消息。
                    if (removeMap.containsKey(i)) {
                        log.info("Half offset {} has been committed/rolled back", i);
                        removeMap.remove(i);
                    } else {
                        //根据消息队列偏移量i从消费队列中获取消息。
                        GetResult getResult = getHalfMsg(messageQueue, i);
                        MessageExt msgExt = getResult.getMsg();
                        if (msgExt == null) {
                            //未获取的消息，则getMessageNullCount+1
                            if (getMessageNullCount++ > MAX_RETRY_COUNT_WHEN_HALF_NULL) {
                                break;
                            }
                            //没有新消息，结束该消息队列的事务状态回查
                            if (getResult.getPullResult().getPullStatus() == PullStatus.NO_NEW_MSG) {
                                log.info("No new msg, the miss offset={} in={}, continue check={}, pull result={}", i,
                                    messageQueue, getMessageNullCount, getResult.getPullResult());
                                break;
                            } else {
                                //其他原因，则更新偏移量i，重新拉取。
                                log.info("Illegal offset, the miss offset={} in={}, continue check={}, pull result={}",
                                    i, messageQueue, getMessageNullCount, getResult.getPullResult());
                                i = getResult.getPullResult().getNextBeginOffset();
                                newOffset = i;
                                continue;
                            }
                        }

                        //判断是否需要丢弃或者跳过此消息
                        if (needDiscard(msgExt, transactionCheckMax) || needSkip(msgExt)) {
                            //记录日志
                            listener.resolveDiscardMsg(msgExt);
                            newOffset = i + 1;
                            i++;
                            continue;
                        }
                        //消息存储时间大于startTime，则结束
                        if (msgExt.getStoreTimestamp() >= startTime) {
                            log.info("Fresh stored. the miss offset={}, check it later, store={}", i,
                                new Date(msgExt.getStoreTimestamp()));
                            break;
                        }

                        /**
                         * 消息已存储的时间，为系统当前时间减去消息出生的时间
                         */
                        long valueOfCurrentMinusBorn = System.currentTimeMillis() - msgExt.getBornTimestamp();
                        /**
                         * 立即检测事务消息的时间，其设计的意义是，应用程序在发送事务消息后，事务不会马上提交，该时间就是假设事务消息发送成功后，
                         * 应用程序事务提交的时间，在这段时间内，RocketMq任务事务未提交，顾不应该在这个时间段向应用程序发送回查请求。
                         */
                        long checkImmunityTime = transactionTimeout;
                        /**
                         * 消息事务消息回查请求的最晚时间，单位为秒，指的是程序发送消息时，可以指定该事务消息的有效时间，
                         * 只有在这个时间内收到回查才有效，默认null
                         */
                        String checkImmunityTimeStr = msgExt.getUserProperty(MessageConst.PROPERTY_CHECK_IMMUNITY_TIME_IN_SECONDS);
                        if (null != checkImmunityTimeStr) {
                            //如果消息指定了事务消息过期时间属性，
                            checkImmunityTime = getImmunityTime(checkImmunityTimeStr, transactionTimeout);
                            //消息已存储的时间小于过期时间，
                            if (valueOfCurrentMinusBorn < checkImmunityTime) {
                                if (checkPrepareQueueOffset(removeMap, doneOpOffset, msgExt, checkImmunityTime)) {
                                    newOffset = i + 1;
                                    i++;
                                    continue;
                                }
                            }
                        } else {
                            //如果当前时间还未过（应用程序结束事务时间），则跳出本次处理时间，等待下一次再试
                            if ((0 <= valueOfCurrentMinusBorn) && (valueOfCurrentMinusBorn < checkImmunityTime)) {
                                log.info("New arrived, the miss offset={}, check it later checkImmunity={}, born={}", i,
                                    checkImmunityTime, new Date(msgExt.getBornTimestamp()));
                                break;
                            }
                        }
                        List<MessageExt> opMsg = pullResult.getMsgFoundList();
                        //
                        /**
                         * 判断是否需要发送事务回查消息
                         * 1.如果事务完成队列(RMQ_SYS_TRANS_OP_HALF_TOPIC)中没有待拉取的消息并且已经超过应用程序事务结束时间即transactionTimeout值
                         * 2.如果从事务完成队列中拉取到消息，并且拉取到的最后一条消息的消息出生时间已经超过transactionTimeout
                         * 因为在下文中，如果isNeedCheck=true，会调用putBackHalfMsgQueue重新将opMsg放入opQueue中，
                         * 重新放入的消息被重置了queueOffSet，commitLogOffSet，即将消费位点前移了，放到opQueue最新一条消息中
                         * 所以如果事务状态回查成功，则fillOpRemoveMap会使得doneOpOffset包含该halfQueue offSet，即使消费位点前移了，后续也不会再重复处理
                         * 如果事务状态回查失败，则判断拉取到的32条消息的最新一条消息存储时间是否超过超时时间，如果是，那肯定是回查失败的，继续进行回查
                         * 3.valueOfCurrentMinusBorn<=-1即事务prepare消息的出生时间大于当前时间？
                         */
                        //@1
                        boolean isNeedCheck = (opMsg == null && valueOfCurrentMinusBorn > checkImmunityTime)
                            || (opMsg != null && (opMsg.get(opMsg.size() - 1).getBornTimestamp() - startTime > transactionTimeout))
                            || (valueOfCurrentMinusBorn <= -1);

                        //如果需要发送事务回查消息
                        if (isNeedCheck) {
                            /**
                             * 则现将消息再次发送到事务准备消息队列。发送成功返回true。
                             *
                             * 在执行事务消息回查之前，会把该消息再次存储到commitlog中，当前消息设置最新的物理偏移量。为什么呢？
                             * 只要是因为下文的发送事务消息回查命令是异步处理的，无法同步返回其处理结果，为了避免简化prepare消息队列和处理队列的
                             * 消息消费进度，先存储，然后消费进度向前推进，重复发送的事务消息在事务回查之前会判断消息是否处理过。
                             * 另外一个目的就是因为需要修改消息的检查次数，RocketMq的存储设计采用的是顺序写，如果去修改已存储的消息，
                             * 其性能无法得到保障。
                             */
                            if (!putBackHalfMsgQueue(msgExt, i)) {
                                continue;
                            }
                            /**
                             * 发送具体的事务回查命令，使用线程池来异步发送回查的消息，为了回查消息消费进度的简化，只需要发送了回查消息，
                             * 当前回查进度也会向前推进(@3标记)，如果回查失败，上一步新增的消息(putBackHalfMsgQueue方法)
                             * 将可以再次发送回查消息，那如果回查消息发送成功，会不会下一次有重复发送回查消息呢？
                             * 这个可以根据OP队列中的消息来判断是否重复，如果回查消息发送成功并且消息服务器完成提交或回滚操作，
                             * 这条消息会发送到OP队列中，然后首先会通过fillOpRemoveMap方法一次拉取32条消息，那又如何保证一定能拉取到与当前
                             * 消息对应的处理记录呢？其实就是通过上面的isNeedCheck(@1标记)的判断逻辑，如果此批消息最后一条未超过事务延迟消息，
                             * 则继续拉取更多消息进行判断(@2,@4标记)，OP队列也会随着回查进度的推进而推进。
                             *
                             */
                            listener.resolveHalfMsg(msgExt);
                        } else {
                            //如果判断不需要发送事务回查消息，则加载更多的已处理消息进行筛选。
                            //@2
                            pullResult = fillOpRemoveMap(removeMap, opQueue, pullResult.getNextBeginOffset(), halfOffset, doneOpOffset);
                            log.info("The miss offset:{} in messageQueue:{} need to get more opMsg, result is:{}", i,
                                messageQueue, pullResult);
                            continue;
                        }
                    }
                    //@3
                    newOffset = i + 1;
                    i++;
                }
                //更新事务prepare消息队列的消费进度
                if (newOffset != halfOffset) {
                    transactionalMessageBridge.updateConsumeOffset(messageQueue, newOffset);
                }
                //更新事务完成消息队列的消费进度
                //@4
                long newOpOffset = calculateOpOffset(doneOpOffset, opOffset);
                if (newOpOffset != opOffset) {
                    transactionalMessageBridge.updateConsumeOffset(opQueue, newOpOffset);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            log.error("Check error", e);
        }

    }

    private long getImmunityTime(String checkImmunityTimeStr, long transactionTimeout) {
        long checkImmunityTime;

        checkImmunityTime = getLong(checkImmunityTimeStr);
        if (-1 == checkImmunityTime) {
            checkImmunityTime = transactionTimeout;
        } else {
            checkImmunityTime *= 1000;
        }
        return checkImmunityTime;
    }

    /**
     * Read op message, parse op message, and fill removeMap
     * 根据当前的处理进度，从已处理队列（事务完成队列）拉取32条消息，方便判断当前处理的消息（事务prepare消息）是否已经处理过，
     * 如果处理过则无需再次发送事务状态回查请求，避免重复发送事务回查请求。
     *
     * @param removeMap Half message to be remove, key:halfOffset, value: opOffset.
     * @param opQueue Op message queue.  事务完成队列
     * @param pullOffsetOfOp The begin offset of op message queue. 事务完成消费队列对应的消费进度
     * @param miniOffset The current minimum offset of half message queue. 事务prepare队列对应的消费进度
     * @param doneOpOffset Stored op messages that have been processed.
     * @return Op message result.
     */
    private PullResult fillOpRemoveMap(HashMap<Long, Long> removeMap,
        MessageQueue opQueue, long pullOffsetOfOp, long miniOffset, List<Long> doneOpOffset) {
        //从事务完成队列中获取32条消息
        PullResult pullResult = pullOpMsg(opQueue, pullOffsetOfOp, 32);
        if (null == pullResult) {
            return null;
        }
        if (pullResult.getPullStatus() == PullStatus.OFFSET_ILLEGAL
            || pullResult.getPullStatus() == PullStatus.NO_MATCHED_MSG) {
            log.warn("The miss op offset={} in queue={} is illegal, pullResult={}", pullOffsetOfOp, opQueue,
                pullResult);
            //没有获取到消息，更新消息消费进度
            transactionalMessageBridge.updateConsumeOffset(opQueue, pullResult.getNextBeginOffset());
            return pullResult;
        } else if (pullResult.getPullStatus() == PullStatus.NO_NEW_MSG) {
            log.warn("The miss op offset={} in queue={} is NO_NEW_MSG, pullResult={}", pullOffsetOfOp, opQueue,
                pullResult);
            return pullResult;
        }
        List<MessageExt> opMsg = pullResult.getMsgFoundList();
        if (opMsg == null) {
            log.warn("The miss op offset={} in queue={} is empty, pullResult={}", pullOffsetOfOp, opQueue, pullResult);
            return pullResult;
        }
        //遍历op队列中的消息
        for (MessageExt opMessageExt : opMsg) {
            /**
             * 当消息写入事务完成消息队列时，其消息体就是之前消息在事务prepare队列中逻辑偏移量（consumequeue中的偏移量）
             * 消息tag就是
             * @see TransactionalMessageUtil#REMOVETAG
             * @see TransactionalMessageBridge#addRemoveTagInTransactionOp(org.apache.rocketmq.common.message.MessageExt, org.apache.rocketmq.common.message.MessageQueue)
             *
             * 在op队列中保存的queueOffset，说明此queueOffset对应的事务prepare中的消息已经被提交或者回滚了，不需要再处理了。
             */
            Long queueOffset = getLong(new String(opMessageExt.getBody(), TransactionalMessageUtil.charset));
            log.info("Topic: {} tags: {}, OpOffset: {}, HalfOffset: {}", opMessageExt.getTopic(),
                opMessageExt.getTags(), opMessageExt.getQueueOffset(), queueOffset);

            if (TransactionalMessageUtil.REMOVETAG.equals(opMessageExt.getTags())) {
                if (queueOffset < miniOffset) {
                    //op队列中的当前消息小于事务prepare消息队列中的消息，说明当此消息对应的事务prepare队列的消息已经处理了，
                    // 放入doneOpOffset，避免重复处理需要更新op队列的消费进度
                    doneOpOffset.add(opMessageExt.getQueueOffset());
                } else {
                    //事务prepare消息队列中 queueOffset 偏移量的消息已经在op队列中存在了说明该事务消息已经提交或者回滚了
                    //放入removeMap中，如果之后事务prepare队列中的offset在此removeMap中，则表示此事务已经完结
                    removeMap.put(queueOffset, opMessageExt.getQueueOffset());
                }
            } else {
                log.error("Found a illegal tag in opMessageExt= {} ", opMessageExt);
            }
        }
        log.debug("Remove map: {}", removeMap);
        log.debug("Done op list: {}", doneOpOffset);
        return pullResult;
    }

    /**
     * If return true, skip this msg
     *
     * @param removeMap Op message map to determine whether a half message was responded by producer.
     * @param doneOpOffset Op Message which has been checked.
     * @param msgExt Half message
     * @param checkImmunityTime User defined time to avoid being detected early.
     * @return Return true if put success, otherwise return false.
     */
    private boolean checkPrepareQueueOffset(HashMap<Long, Long> removeMap, List<Long> doneOpOffset, MessageExt msgExt,
        long checkImmunityTime) {
        if (System.currentTimeMillis() - msgExt.getBornTimestamp() < checkImmunityTime) {
            String prepareQueueOffsetStr = msgExt.getUserProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED_QUEUE_OFFSET);
            if (null == prepareQueueOffsetStr) {
                return putImmunityMsgBackToHalfQueue(msgExt);
            } else {
                long prepareQueueOffset = getLong(prepareQueueOffsetStr);
                if (-1 == prepareQueueOffset) {
                    return false;
                } else {
                    if (removeMap.containsKey(prepareQueueOffset)) {
                        long tmpOpOffset = removeMap.remove(prepareQueueOffset);
                        doneOpOffset.add(tmpOpOffset);
                        return true;
                    } else {
                        return putImmunityMsgBackToHalfQueue(msgExt);
                    }
                }

            }

        } else {
            return true;
        }
    }

    /**
     * Write messageExt to Half topic again
     *
     * @param messageExt Message will be write back to queue
     * @return Put result can used to determine the specific results of storage.
     */
    private PutMessageResult putBackToHalfQueueReturnResult(MessageExt messageExt) {
        PutMessageResult putMessageResult = null;
        try {
            MessageExtBrokerInner msgInner = transactionalMessageBridge.renewHalfMessageInner(messageExt);
            putMessageResult = transactionalMessageBridge.putMessageReturnResult(msgInner);
        } catch (Exception e) {
            log.warn("PutBackToHalfQueueReturnResult error", e);
        }
        return putMessageResult;
    }

    private boolean putImmunityMsgBackToHalfQueue(MessageExt messageExt) {
        MessageExtBrokerInner msgInner = transactionalMessageBridge.renewImmunityHalfMessageInner(messageExt);
        return transactionalMessageBridge.putMessage(msgInner);
    }

    /**
     * Read half message from Half Topic
     *
     * @param mq Target message queue, in this method, it means the half message queue.
     * @param offset Offset in the message queue.
     * @param nums Pull message number.
     * @return Messages pulled from half message queue.
     */
    private PullResult pullHalfMsg(MessageQueue mq, long offset, int nums) {
        return transactionalMessageBridge.getHalfMessage(mq.getQueueId(), offset, nums);
    }

    /**
     * Read op message from Op Topic
     *
     * @param mq Target Message Queue
     * @param offset Offset in the message queue
     * @param nums Pull message number
     * @return Messages pulled from operate message queue.
     */
    private PullResult pullOpMsg(MessageQueue mq, long offset, int nums) {
        return transactionalMessageBridge.getOpMessage(mq.getQueueId(), offset, nums);
    }

    private Long getLong(String s) {
        long v = -1;
        try {
            v = Long.valueOf(s);
        } catch (Exception e) {
            log.error("GetLong error", e);
        }
        return v;

    }

    private Integer getInt(String s) {
        int v = -1;
        try {
            v = Integer.valueOf(s);
        } catch (Exception e) {
            log.error("GetInt error", e);
        }
        return v;

    }

    private long calculateOpOffset(List<Long> doneOffset, long oldOffset) {
        Collections.sort(doneOffset);
        long newOffset = oldOffset;
        for (int i = 0; i < doneOffset.size(); i++) {
            if (doneOffset.get(i) == newOffset) {
                newOffset++;
            } else {
                break;
            }
        }
        return newOffset;

    }

    private MessageQueue getOpQueue(MessageQueue messageQueue) {
        MessageQueue opQueue = opQueueMap.get(messageQueue);
        if (opQueue == null) {
            opQueue = new MessageQueue(TransactionalMessageUtil.buildOpTopic(), messageQueue.getBrokerName(),
                messageQueue.getQueueId());
            opQueueMap.put(messageQueue, opQueue);
        }
        return opQueue;

    }

    private GetResult getHalfMsg(MessageQueue messageQueue, long offset) {
        GetResult getResult = new GetResult();

        PullResult result = pullHalfMsg(messageQueue, offset, PULL_MSG_RETRY_NUMBER);
        getResult.setPullResult(result);
        List<MessageExt> messageExts = result.getMsgFoundList();
        if (messageExts == null) {
            return getResult;
        }
        getResult.setMsg(messageExts.get(0));
        return getResult;
    }

    private OperationResult getHalfMessageByOffset(long commitLogOffset) {
        OperationResult response = new OperationResult();
        MessageExt messageExt = this.transactionalMessageBridge.lookMessageByOffset(commitLogOffset);
        if (messageExt != null) {
            response.setPrepareMessage(messageExt);
            response.setResponseCode(ResponseCode.SUCCESS);
        } else {
            response.setResponseCode(ResponseCode.SYSTEM_ERROR);
            response.setResponseRemark("Find prepared transaction message failed");
        }
        return response;
    }

    @Override
    public boolean deletePrepareMessage(MessageExt msgExt) {
        if (this.transactionalMessageBridge.putOpMessage(msgExt, TransactionalMessageUtil.REMOVETAG)) {
            log.info("Transaction op message write successfully. messageId={}, queueId={} msgExt:{}", msgExt.getMsgId(), msgExt.getQueueId(), msgExt);
            return true;
        } else {
            log.error("Transaction op message write failed. messageId is {}, queueId is {}", msgExt.getMsgId(), msgExt.getQueueId());
            return false;
        }
    }

    @Override
    public OperationResult commitMessage(EndTransactionRequestHeader requestHeader) {
        return getHalfMessageByOffset(requestHeader.getCommitLogOffset());
    }

    @Override
    public OperationResult rollbackMessage(EndTransactionRequestHeader requestHeader) {
        return getHalfMessageByOffset(requestHeader.getCommitLogOffset());
    }

    @Override
    public boolean open() {
        return true;
    }

    @Override
    public void close() {

    }

}

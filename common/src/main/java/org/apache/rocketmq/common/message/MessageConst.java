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
package org.apache.rocketmq.common.message;

import org.apache.rocketmq.common.MixAll;

import java.util.HashSet;

public class MessageConst {

    /**
     * 消息扩展属性
     */

    /**
     * Message索引键，多个用空格隔开，RocketMQ可以根据这些key快速检索到消息
     */
    public static final String PROPERTY_KEYS = "KEYS";
    /**
     * 消息TAG，用于消息过滤
     */
    public static final String PROPERTY_TAGS = "TAGS";
    /**
     * 如果是同步刷盘的策略是否等消息存储完成后再返回结果
     * @see org.apache.rocketmq.store.CommitLog#handleDiskFlush(org.apache.rocketmq.store.AppendMessageResult, org.apache.rocketmq.store.PutMessageResult, org.apache.rocketmq.common.message.MessageExt)
     * 如果当前Broker是同步双写Broke{@link org.apache.rocketmq.store.config.MessageStoreConfig#brokerRole}是否等消息存储完成后再返回结果
     * @see org.apache.rocketmq.store.CommitLog#handleHA(org.apache.rocketmq.store.AppendMessageResult, org.apache.rocketmq.store.PutMessageResult, org.apache.rocketmq.common.message.MessageExt)
     * @see Message#isWaitStoreMsgOK() 未设置默的话默认true
     */
    public static final String PROPERTY_WAIT_STORE_MSG_OK = "WAIT";
    /**
     * 消息延迟级别，用于定时消息或者重试消息。
     * {@link org.apache.rocketmq.store.config.MessageStoreConfig#messageDelayLevel}
     */
    public static final String PROPERTY_DELAY_TIME_LEVEL = "DELAY";

    /**
     * 当消息是重试消息时，消息的此属性存储消息的真实的topic
     * @see org.apache.rocketmq.broker.processor.SendMessageProcessor#consumerSendMsgBack(io.netty.channel.ChannelHandlerContext, org.apache.rocketmq.remoting.protocol.RemotingCommand)
     */
    public static final String PROPERTY_RETRY_TOPIC = "RETRY_TOPIC";

    /**
     * 当是延迟消息时需要将消息的真实topic存储到消息的扩展属性中，
     * PROPERTY_REAL_TOPIC为对应的key
     * {@link org.apache.rocketmq.store.CommitLog#putMessage(org.apache.rocketmq.store.MessageExtBrokerInner)}
     *
     * 当是事务消息时需要将消息的真实topic存储到消息的扩展属性中，
     * {@link org.apache.rocketmq.broker.transaction.queue.TransactionalMessageBridge#parseHalfMessageInner(org.apache.rocketmq.store.MessageExtBrokerInner)}
     */
    public static final String PROPERTY_REAL_TOPIC = "REAL_TOPIC";
    /**
     * 当是延迟消息时需要将消息的真实队列ID存储到消息的扩展属性中，
     * PROPERTY_REAL_QUEUE_ID为对应的key
     * @see org.apache.rocketmq.store.CommitLog#putMessage(org.apache.rocketmq.store.MessageExtBrokerInner)
     *
     * 当是事务消息时需要将消息的真实队列ID存储到消息的扩展属性中，
     * {@link org.apache.rocketmq.broker.transaction.queue.TransactionalMessageBridge#parseHalfMessageInner(org.apache.rocketmq.store.MessageExtBrokerInner)}
     */
    public static final String PROPERTY_REAL_QUEUE_ID = "REAL_QID";
    /**
     * @see org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl#sendMessageInTransaction(org.apache.rocketmq.common.message.Message, org.apache.rocketmq.client.producer.TransactionListener, java.lang.Object)
     * 中赋值
     *  message.getProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED);
     *  如果得到true(忽略大小写，那么代表事务PREPARED消息)
     */
    public static final String PROPERTY_TRANSACTION_PREPARED = "TRAN_MSG";
    /**
     * @see org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl#sendMessageInTransaction(org.apache.rocketmq.common.message.Message, org.apache.rocketmq.client.producer.TransactionListener, java.lang.Object)
     * 事务消息所属消息生产者组，设置消息生产者组的目的是在查询事务消息本地事务状态时，从该生产者组中随机选择一个消息生产者即可。
     */
    public static final String PROPERTY_PRODUCER_GROUP = "PGROUP";
    /**
     * 消息消费队列文件（ConsumeQueue）中的最小偏移量
     * @see org.apache.rocketmq.client.impl.consumer.PullAPIWrapper#processPullResult(org.apache.rocketmq.common.message.MessageQueue, org.apache.rocketmq.client.consumer.PullResult, org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData)
     */
    public static final String PROPERTY_MIN_OFFSET = "MIN_OFFSET";
    /**
     * 消息消费队列文件（ConsumeQueue）中的最大偏移量
     * @see org.apache.rocketmq.client.impl.consumer.PullAPIWrapper#processPullResult(org.apache.rocketmq.common.message.MessageQueue, org.apache.rocketmq.client.consumer.PullResult, org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData)
     */
    public static final String PROPERTY_MAX_OFFSET = "MAX_OFFSET";
    public static final String PROPERTY_BUYER_ID = "BUYER_ID";
    /**
     * 重试消息时，新创建一个消息，此消息的属性保存原先消息的msgId
     */
    public static final String PROPERTY_ORIGIN_MESSAGE_ID = "ORIGIN_MESSAGE_ID";
    public static final String PROPERTY_TRANSFER_FLAG = "TRANSFER_FLAG";
    public static final String PROPERTY_CORRECTION_FLAG = "CORRECTION_FLAG";
    public static final String PROPERTY_MQ2_FLAG = "MQ2_FLAG";
    /**
     * 消息重新消费次数
     */
    public static final String PROPERTY_RECONSUME_TIME = "RECONSUME_TIME";
    public static final String PROPERTY_MSG_REGION = "MSG_REGION";
    public static final String PROPERTY_TRACE_SWITCH = "TRACE_ON";
    /**
     * 消息唯一Id的key
     * {@link MessageClientIDSetter#setUniqID(org.apache.rocketmq.common.message.Message)}
     * {@link org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl#sendKernelImpl(org.apache.rocketmq.common.message.Message, org.apache.rocketmq.common.message.MessageQueue, org.apache.rocketmq.client.impl.CommunicationMode, org.apache.rocketmq.client.producer.SendCallback, org.apache.rocketmq.client.impl.producer.TopicPublishInfo, long)}
     */
    public static final String PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX = "UNIQ_KEY";
    /**
     * 消息最大重新消费次数
     */
    public static final String PROPERTY_MAX_RECONSUME_TIMES = "MAX_RECONSUME_TIMES";
    public static final String PROPERTY_CONSUME_START_TIMESTAMP = "CONSUME_START_TIME";
    public static final String PROPERTY_TRANSACTION_PREPARED_QUEUE_OFFSET = "TRAN_PREPARED_QUEUE_OFFSET";
    /**
     * 事务消息的回查次数，消息属性中的key
     * @see org.apache.rocketmq.broker.transaction.queue.TransactionalMessageServiceImpl#needDiscard(org.apache.rocketmq.common.message.MessageExt, int)
     */
    public static final String PROPERTY_TRANSACTION_CHECK_TIMES = "TRANSACTION_CHECK_TIMES";
    public static final String PROPERTY_CHECK_IMMUNITY_TIME_IN_SECONDS = "CHECK_IMMUNITY_TIME_IN_SECONDS";

    public static final String KEY_SEPARATOR = " ";

    public static final HashSet<String> STRING_HASH_SET = new HashSet<String>();

    static {
        STRING_HASH_SET.add(PROPERTY_TRACE_SWITCH);
        STRING_HASH_SET.add(PROPERTY_MSG_REGION);
        STRING_HASH_SET.add(PROPERTY_KEYS);
        STRING_HASH_SET.add(PROPERTY_TAGS);
        STRING_HASH_SET.add(PROPERTY_WAIT_STORE_MSG_OK);
        STRING_HASH_SET.add(PROPERTY_DELAY_TIME_LEVEL);
        STRING_HASH_SET.add(PROPERTY_RETRY_TOPIC);
        STRING_HASH_SET.add(PROPERTY_REAL_TOPIC);
        STRING_HASH_SET.add(PROPERTY_REAL_QUEUE_ID);
        STRING_HASH_SET.add(PROPERTY_TRANSACTION_PREPARED);
        STRING_HASH_SET.add(PROPERTY_PRODUCER_GROUP);
        STRING_HASH_SET.add(PROPERTY_MIN_OFFSET);
        STRING_HASH_SET.add(PROPERTY_MAX_OFFSET);
        STRING_HASH_SET.add(PROPERTY_BUYER_ID);
        STRING_HASH_SET.add(PROPERTY_ORIGIN_MESSAGE_ID);
        STRING_HASH_SET.add(PROPERTY_TRANSFER_FLAG);
        STRING_HASH_SET.add(PROPERTY_CORRECTION_FLAG);
        STRING_HASH_SET.add(PROPERTY_MQ2_FLAG);
        STRING_HASH_SET.add(PROPERTY_RECONSUME_TIME);
        STRING_HASH_SET.add(PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
        STRING_HASH_SET.add(PROPERTY_MAX_RECONSUME_TIMES);
        STRING_HASH_SET.add(PROPERTY_CONSUME_START_TIMESTAMP);
    }
}

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
package org.apache.rocketmq.common;

import java.net.InetAddress;
import java.net.UnknownHostException;
import org.apache.rocketmq.common.annotation.ImportantField;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingUtil;

public class BrokerConfig {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    private String rocketmqHome = System.getProperty(MixAll.ROCKETMQ_HOME_PROPERTY, System.getenv(MixAll.ROCKETMQ_HOME_ENV));
    @ImportantField
    private String namesrvAddr = System.getProperty(MixAll.NAMESRV_ADDR_PROPERTY, System.getenv(MixAll.NAMESRV_ADDR_ENV));
    @ImportantField
    private String brokerIP1 = RemotingUtil.getLocalAddress();
    private String brokerIP2 = RemotingUtil.getLocalAddress();
    @ImportantField
    private String brokerName = localHostName();
    @ImportantField
    private String brokerClusterName = "DefaultCluster";
    @ImportantField
    private long brokerId = MixAll.MASTER_ID;
    private int brokerPermission = PermName.PERM_READ | PermName.PERM_WRITE;
    private int defaultTopicQueueNums = 8;
    /**
     * 当topic没有预先创建时。
     * 在Producer发消息时是否自动创建topic
     * @see org.apache.rocketmq.broker.processor.AbstractSendMessageProcessor#msgCheck(io.netty.channel.ChannelHandlerContext, org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader, org.apache.rocketmq.remoting.protocol.RemotingCommand)
     *
     *
     * 开启自动创建topic可能带来的问题
     *
     * 因为开启了自动创建路由信息，消息发送者根据Topic去NameServer无法得到路由信息，
     * 但接下来根据默认Topic从NameServer是能拿到路由信息(在每个Broker中，存在8个队列)，
     * @see org.apache.rocketmq.broker.topic.TopicConfigManager#TopicConfigManager(org.apache.rocketmq.broker.BrokerController)
     * 因为两个Broker在启动时都会向NameServer汇报路由信息。此时消息发送者缓存的默认topic的路由信息是2个Broker，
     * 每个Broker默认4个队列。本来默认topic是8个读写队列的，但是在下面方法中改为4个了
     * @see org.apache.rocketmq.client.impl.factory.MQClientInstance#updateTopicRouteInfoFromNameServer(java.lang.String, boolean, org.apache.rocketmq.client.producer.DefaultMQProducer)
     * 消息发送者然后按照轮询机制，发送第一条消息选择(broker-a的messageQueue:0)，
     * 向Broker发送消息，Broker服务器在处理消息时，首先会查看自己的路由配置管理器(TopicConfigManager)中的路由信息，
     * 此时不存在对应的路由信息，然后尝试查询是否存在默认Topic的路由信息，如果存在，说明启用了autoCreateTopicEnable，
     * 则在TopicConfigManager中创建新Topic的路由信息，此时存在与Broker服务端的内存中，然后本次消息发送结束。
     * @see org.apache.rocketmq.broker.processor.AbstractSendMessageProcessor#msgCheck(io.netty.channel.ChannelHandlerContext, org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader, org.apache.rocketmq.remoting.protocol.RemotingCommand)
     * 此时，在NameServer中还不存在新创建的Topic的路由信息。
     *
     * 这里有三个关键点：
     *
     * 1. 启用autoCreateTopicEnable创建主题时，在Broker端创建主题的时机为，消息生产者往Broker端发送消息时才会创建。
     * 2. 然后Broker端会在一个心跳包周期内，将新创建的路由信息发送到NameServer，于此同时，Broker端还会有一个定时任务，定时将内存中的路由信息，持久化到Broker端的磁盘上。
     *    注意这里Broker向namesrv发送心跳包后，namesrv缓存了自动创建的topic在broker-a上的路由信息，此时broker-b上还未创建此topic。
     * 3. 消息发送者会每隔30s向NameServer更新路由信息，如果消息发送端一段时间内未发送消息（此时消息消费者从namesrv获取到的topic的路由信息中只有broker-a），
     *    就不会有消息发送集群内的第二台Broker，那么NameServer中新创建的Topic的路由信息只会包含Broker-a，
     *    然后消息发送者会向NameServer拉取最新的路由信息，此时就会消息发送者原本缓存了2个broker的路由信息，将会变为一个Broker的路由信息，
     *    则该Topic的消息永远不会发送到另外一个Broker，就出现了上述现象。
     *
     * 原因就分析到这里了，现在我们还可以的大胆假设，开启autoCreateTopicEnable机制，什么情况会在两个Broker上都创建队列，其实，我们只需要连续快速的发送9条消息，就有可能在2个Broker上都创建队列，
     * 因为默认topic是8个写队列，那么发送快速9条消息，根据消息队列负载均衡算法，最终会往前九个队列均发送一条消息，而第9个队列是在broker-b上，因此根据上面的逻辑
     * broker-b上也会创建此topic的信息，心跳机制至namesrv，最终生产者就能获得两个broker的路由信息
     */
    @ImportantField
    private boolean autoCreateTopicEnable = true;

    private boolean clusterTopicEnable = true;

    private boolean brokerTopicEnable = true;
    /**
     * 是否自动创建消费组配置信息
     * @see org.apache.rocketmq.broker.subscription.SubscriptionGroupManager#findSubscriptionGroupConfig(java.lang.String)
     * @see org.apache.rocketmq.broker.processor.ClientManageProcessor#heartBeat(io.netty.channel.ChannelHandlerContext, org.apache.rocketmq.remoting.protocol.RemotingCommand)
     */
    @ImportantField
    private boolean autoCreateSubscriptionGroup = true;
    private String messageStorePlugIn = "";

    /**
     * thread numbers for send message thread pool, since spin lock will be used by default since 4.0.x, the default
     * value is 1.
     */
    private int sendMessageThreadPoolNums = 1; //16 + Runtime.getRuntime().availableProcessors() * 4;
    private int pullMessageThreadPoolNums = 16 + Runtime.getRuntime().availableProcessors() * 2;
    private int queryMessageThreadPoolNums = 8 + Runtime.getRuntime().availableProcessors();

    private int adminBrokerThreadPoolNums = 16;
    private int clientManageThreadPoolNums = 32;
    private int consumerManageThreadPoolNums = 32;
    private int heartbeatThreadPoolNums = Math.min(32,Runtime.getRuntime().availableProcessors());

    private int flushConsumerOffsetInterval = 1000 * 5;

    private int flushConsumerOffsetHistoryInterval = 1000 * 60;

    /**
     * broker是否拒绝事务消息
     * false-不拒绝，也就是支持事务消息
     * @see org.apache.rocketmq.broker.processor.SendMessageProcessor#sendMessage(io.netty.channel.ChannelHandlerContext, org.apache.rocketmq.remoting.protocol.RemotingCommand, org.apache.rocketmq.broker.mqtrace.SendMessageContext, org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader)
     */
    @ImportantField
    private boolean rejectTransactionMessage = false;
    @ImportantField
    private boolean fetchNamesrvAddrByAddressServer = false;
    private int sendThreadPoolQueueCapacity = 10000;
    private int pullThreadPoolQueueCapacity = 100000;
    private int queryThreadPoolQueueCapacity = 20000;
    private int clientManagerThreadPoolQueueCapacity = 1000000;
    private int consumerManagerThreadPoolQueueCapacity = 1000000;
    private int heartbeatThreadPoolQueueCapacity = 50000;

    private int filterServerNums = 0;

    /**
     * 是否开始长轮询
     * RocketMQ的消息消费方式，采用了“长轮询”方式，兼具了Push和Pull的有点，不过需要Server和Client的配合才能够实现。
     * 即Client发送消息请求，Server端接受请求，如果发现Server队列里没有新消息，Server端不立即返回，而是持有这个请求一段时间（通过设置超时时间来实现），
     * 在这段时间内轮询Server队列内是否有新的消息，如果有新消息，就利用现有的连接返回消息给消费者；如果这段时间内没有新消息进入队列，则返回空。
     * 这样消费消息的主动权既保留在Client端，也不会出现Server积压大量消息后，短时间内推送给Client大量消息使client因为性能问题出现消费不及时的情况。
     *
     * 长轮询的弊端：在持有消费者请求的这段时间，占用了系统资源，因此长轮询适合客户端连接数可控的业务场景中。
     */
    private boolean longPollingEnable = true;

    /**
     * @see BrokerConfig#longPollingEnable 为false时
     * 服务器端默认持有请求的时间，即1s
     */
    private long shortPollingTimeMills = 1000;

    /**
     * 当有新的消费者或者消费组订阅信息变化时时是否立马通知客户端进行消息队列的负载均衡
     * @see org.apache.rocketmq.broker.client.ConsumerManager#registerConsumer(java.lang.String, org.apache.rocketmq.broker.client.ClientChannelInfo, org.apache.rocketmq.common.protocol.heartbeat.ConsumeType, org.apache.rocketmq.common.protocol.heartbeat.MessageModel, org.apache.rocketmq.common.consumer.ConsumeFromWhere, java.util.Set, boolean)
     */
    private boolean notifyConsumerIdsChangedEnable = true;

    private boolean highSpeedMode = false;

    private boolean commercialEnable = true;
    private int commercialTimerCount = 1;
    private int commercialTransCount = 1;
    private int commercialBigCount = 1;
    private int commercialBaseCount = 1;

    private boolean transferMsgByHeap = true;
    private int maxDelayTime = 40;

    private String regionId = MixAll.DEFAULT_TRACE_REGION_ID;
    private int registerBrokerTimeoutMills = 6000;

    /**
     * 从节点是否可读
     * @see org.apache.rocketmq.broker.processor.PullMessageProcessor#processRequest(io.netty.channel.Channel, org.apache.rocketmq.remoting.protocol.RemotingCommand, boolean)
     */
    private boolean slaveReadEnable = false;

    private boolean disableConsumeIfConsumerReadSlowly = false;
    private long consumerFallbehindThreshold = 1024L * 1024 * 1024 * 16;

    /**
     * 1. 消息发送者向 Broker 发送消息写入请求，Broker 端在接收到请求后会首先放入一个队列中(SendThreadPoolQueue)，默认容量为 10000。
     * 2. Broker 会专门使用一个线程池{@link org.apache.rocketmq.broker.BrokerController#sendMessageExecutor}去从队列中获取任务并执行消息写入请求，为了保证消息的顺序处理，该线程池默认线程个数为1。
     *
     * 如果 Broker 端受到垃圾回收等等因素造成单条写入数据发生抖动，单个 Broker 端积压的请求太多从而得不到及时处理，会极大的造成客户端消息发送的时间延长。
     *
     * 设想一下，如果由于 Broker 压力增大，写入一条消息需要500ms甚至超过1s，并且队列中积压了5000条消息，消息发送端的默认超时时间为3s，
     * 如果按照这样的速度，这些请求在轮到 Broker 执行写入请求时，客户端已经将这个请求超时了，这样不仅会造成大量的无效处理，还会导致客户端发送超时。
     *
     * 故 RocketMQ 为了解决该问题，引入 Broker 端快速失败机制，即开启一个定时调度线程，
     * 每隔10毫秒{@link org.apache.rocketmq.broker.latency.BrokerFastFailure#start()}去检查队列中的第一个排队节点，如果该节点的排队时间已经超过了{@link BrokerConfig#waitTimeMillsInSendQueue}，
     * 就会取消该队列中所有已超过 200ms 的请求，立即向客户端返回失败，这样客户端能尽快进行重试，因为 Broker 都是集群部署，下次重试可以发送到其他 Broker 上，
     * 这样能最大程度保证消息发送在默认 3s 的时间内经过重试机制，能有效避免某一台 Broker 由于瞬时压力大而造成的消息发送不可用，从而实现消息发送的高可用。
     */
    private boolean brokerFastFailureEnable = true;
    private long waitTimeMillsInSendQueue = 200;
    private long waitTimeMillsInPullQueue = 5 * 1000;
    private long waitTimeMillsInHeartbeatQueue = 31 * 1000;

    private long startAcceptSendRequestTimeStamp = 0L;

    private boolean traceOn = true;

    // Switch of filter bit map calculation.
    // If switch on:
    // 1. Calculate filter bit map when construct queue.
    // 2. Filter bit map will be saved to consume queue extend file if allowed.
    private boolean enableCalcFilterBitMap = false;

    // Expect num of consumers will use filter.
    private int expectConsumerNumUseFilter = 32;

    // Error rate of bloom filter, 1~100.
    private int maxErrorRateOfBloomFilter = 20;

    //how long to clean filter data after dead.Default: 24h
    private long filterDataCleanTimeSpan = 24 * 3600 * 1000;

    // whether do filter when retry.
    /**
     * 是否支持重试消息的topic过滤
     * @see org.apache.rocketmq.broker.processor.PullMessageProcessor#processRequest(io.netty.channel.Channel, org.apache.rocketmq.remoting.protocol.RemotingCommand, boolean)
     */
    private boolean filterSupportRetry = false;
    private boolean enablePropertyFilter = false;

    private boolean compressedRegister = false;

    private boolean forceRegister = true;

    /**
     * This configurable item defines interval of topics registration of broker to name server. Allowing values are
     * between 10, 000 and 60, 000 milliseconds.
     */
    private int registerNameServerPeriod = 1000 * 30;

    /**
     * The minimum time of the transactional message  to be checked firstly, one message only exceed this time interval
     * that can be checked.
     * 事务过期时间，
     * 只有当消息存储时间 + transactionTimeOut 大于系统当前时间，
     * 才对消息执行事务状态回查，否则在下一周期中执行事务回查操作
     * @see org.apache.rocketmq.broker.transaction.TransactionalMessageCheckService#onWaitEnd()
     */
    @ImportantField
    private long transactionTimeOut = 3 * 1000;

    /**
     * The maximum number of times the message was checked, if exceed this value, this message will be discarded.
     * 事务消息回查最大检测次数
     * 如果超过最大检测次数还是无法获知消息的事务状态，将不会继续对消息进行事务回查，而是直接丢弃，相当于事务回滚。
     * @see org.apache.rocketmq.broker.transaction.TransactionalMessageCheckService#onWaitEnd()
     */
    @ImportantField
    private int transactionCheckMax = 5;

    /**
     * Transaction message check interval.
     * 检测事务消息的状态的定时任务间隔
     */
    @ImportantField
    private long transactionCheckInterval = 60 * 1000;

    public boolean isTraceOn() {
        return traceOn;
    }

    public void setTraceOn(final boolean traceOn) {
        this.traceOn = traceOn;
    }

    public long getStartAcceptSendRequestTimeStamp() {
        return startAcceptSendRequestTimeStamp;
    }

    public void setStartAcceptSendRequestTimeStamp(final long startAcceptSendRequestTimeStamp) {
        this.startAcceptSendRequestTimeStamp = startAcceptSendRequestTimeStamp;
    }

    public long getWaitTimeMillsInSendQueue() {
        return waitTimeMillsInSendQueue;
    }

    public void setWaitTimeMillsInSendQueue(final long waitTimeMillsInSendQueue) {
        this.waitTimeMillsInSendQueue = waitTimeMillsInSendQueue;
    }

    public long getConsumerFallbehindThreshold() {
        return consumerFallbehindThreshold;
    }

    public void setConsumerFallbehindThreshold(final long consumerFallbehindThreshold) {
        this.consumerFallbehindThreshold = consumerFallbehindThreshold;
    }

    public boolean isBrokerFastFailureEnable() {
        return brokerFastFailureEnable;
    }

    public void setBrokerFastFailureEnable(final boolean brokerFastFailureEnable) {
        this.brokerFastFailureEnable = brokerFastFailureEnable;
    }

    public long getWaitTimeMillsInPullQueue() {
        return waitTimeMillsInPullQueue;
    }

    public void setWaitTimeMillsInPullQueue(final long waitTimeMillsInPullQueue) {
        this.waitTimeMillsInPullQueue = waitTimeMillsInPullQueue;
    }

    public boolean isDisableConsumeIfConsumerReadSlowly() {
        return disableConsumeIfConsumerReadSlowly;
    }

    public void setDisableConsumeIfConsumerReadSlowly(final boolean disableConsumeIfConsumerReadSlowly) {
        this.disableConsumeIfConsumerReadSlowly = disableConsumeIfConsumerReadSlowly;
    }

    public boolean isSlaveReadEnable() {
        return slaveReadEnable;
    }

    public void setSlaveReadEnable(final boolean slaveReadEnable) {
        this.slaveReadEnable = slaveReadEnable;
    }

    public static String localHostName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            log.error("Failed to obtain the host name", e);
        }

        return "DEFAULT_BROKER";
    }

    public int getRegisterBrokerTimeoutMills() {
        return registerBrokerTimeoutMills;
    }

    public void setRegisterBrokerTimeoutMills(final int registerBrokerTimeoutMills) {
        this.registerBrokerTimeoutMills = registerBrokerTimeoutMills;
    }

    public String getRegionId() {
        return regionId;
    }

    public void setRegionId(final String regionId) {
        this.regionId = regionId;
    }

    public boolean isTransferMsgByHeap() {
        return transferMsgByHeap;
    }

    public void setTransferMsgByHeap(final boolean transferMsgByHeap) {
        this.transferMsgByHeap = transferMsgByHeap;
    }

    public String getMessageStorePlugIn() {
        return messageStorePlugIn;
    }

    public void setMessageStorePlugIn(String messageStorePlugIn) {
        this.messageStorePlugIn = messageStorePlugIn;
    }

    public boolean isHighSpeedMode() {
        return highSpeedMode;
    }

    public void setHighSpeedMode(final boolean highSpeedMode) {
        this.highSpeedMode = highSpeedMode;
    }

    public String getRocketmqHome() {
        return rocketmqHome;
    }

    public void setRocketmqHome(String rocketmqHome) {
        this.rocketmqHome = rocketmqHome;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }

    public int getBrokerPermission() {
        return brokerPermission;
    }

    public void setBrokerPermission(int brokerPermission) {
        this.brokerPermission = brokerPermission;
    }

    public int getDefaultTopicQueueNums() {
        return defaultTopicQueueNums;
    }

    public void setDefaultTopicQueueNums(int defaultTopicQueueNums) {
        this.defaultTopicQueueNums = defaultTopicQueueNums;
    }

    public boolean isAutoCreateTopicEnable() {
        return autoCreateTopicEnable;
    }

    public void setAutoCreateTopicEnable(boolean autoCreateTopic) {
        this.autoCreateTopicEnable = autoCreateTopic;
    }

    public String getBrokerClusterName() {
        return brokerClusterName;
    }

    public void setBrokerClusterName(String brokerClusterName) {
        this.brokerClusterName = brokerClusterName;
    }

    public String getBrokerIP1() {
        return brokerIP1;
    }

    public void setBrokerIP1(String brokerIP1) {
        this.brokerIP1 = brokerIP1;
    }

    public String getBrokerIP2() {
        return brokerIP2;
    }

    public void setBrokerIP2(String brokerIP2) {
        this.brokerIP2 = brokerIP2;
    }

    public int getSendMessageThreadPoolNums() {
        return sendMessageThreadPoolNums;
    }

    public void setSendMessageThreadPoolNums(int sendMessageThreadPoolNums) {
        this.sendMessageThreadPoolNums = sendMessageThreadPoolNums;
    }

    public int getPullMessageThreadPoolNums() {
        return pullMessageThreadPoolNums;
    }

    public void setPullMessageThreadPoolNums(int pullMessageThreadPoolNums) {
        this.pullMessageThreadPoolNums = pullMessageThreadPoolNums;
    }

    public int getQueryMessageThreadPoolNums() {
        return queryMessageThreadPoolNums;
    }

    public void setQueryMessageThreadPoolNums(final int queryMessageThreadPoolNums) {
        this.queryMessageThreadPoolNums = queryMessageThreadPoolNums;
    }

    public int getAdminBrokerThreadPoolNums() {
        return adminBrokerThreadPoolNums;
    }

    public void setAdminBrokerThreadPoolNums(int adminBrokerThreadPoolNums) {
        this.adminBrokerThreadPoolNums = adminBrokerThreadPoolNums;
    }

    public int getFlushConsumerOffsetInterval() {
        return flushConsumerOffsetInterval;
    }

    public void setFlushConsumerOffsetInterval(int flushConsumerOffsetInterval) {
        this.flushConsumerOffsetInterval = flushConsumerOffsetInterval;
    }

    public int getFlushConsumerOffsetHistoryInterval() {
        return flushConsumerOffsetHistoryInterval;
    }

    public void setFlushConsumerOffsetHistoryInterval(int flushConsumerOffsetHistoryInterval) {
        this.flushConsumerOffsetHistoryInterval = flushConsumerOffsetHistoryInterval;
    }

    public boolean isClusterTopicEnable() {
        return clusterTopicEnable;
    }

    public void setClusterTopicEnable(boolean clusterTopicEnable) {
        this.clusterTopicEnable = clusterTopicEnable;
    }

    public String getNamesrvAddr() {
        return namesrvAddr;
    }

    public void setNamesrvAddr(String namesrvAddr) {
        this.namesrvAddr = namesrvAddr;
    }

    public long getBrokerId() {
        return brokerId;
    }

    public void setBrokerId(long brokerId) {
        this.brokerId = brokerId;
    }

    public boolean isAutoCreateSubscriptionGroup() {
        return autoCreateSubscriptionGroup;
    }

    public void setAutoCreateSubscriptionGroup(boolean autoCreateSubscriptionGroup) {
        this.autoCreateSubscriptionGroup = autoCreateSubscriptionGroup;
    }

    public boolean isRejectTransactionMessage() {
        return rejectTransactionMessage;
    }

    public void setRejectTransactionMessage(boolean rejectTransactionMessage) {
        this.rejectTransactionMessage = rejectTransactionMessage;
    }

    public boolean isFetchNamesrvAddrByAddressServer() {
        return fetchNamesrvAddrByAddressServer;
    }

    public void setFetchNamesrvAddrByAddressServer(boolean fetchNamesrvAddrByAddressServer) {
        this.fetchNamesrvAddrByAddressServer = fetchNamesrvAddrByAddressServer;
    }

    public int getSendThreadPoolQueueCapacity() {
        return sendThreadPoolQueueCapacity;
    }

    public void setSendThreadPoolQueueCapacity(int sendThreadPoolQueueCapacity) {
        this.sendThreadPoolQueueCapacity = sendThreadPoolQueueCapacity;
    }

    public int getPullThreadPoolQueueCapacity() {
        return pullThreadPoolQueueCapacity;
    }

    public void setPullThreadPoolQueueCapacity(int pullThreadPoolQueueCapacity) {
        this.pullThreadPoolQueueCapacity = pullThreadPoolQueueCapacity;
    }

    public int getQueryThreadPoolQueueCapacity() {
        return queryThreadPoolQueueCapacity;
    }

    public void setQueryThreadPoolQueueCapacity(final int queryThreadPoolQueueCapacity) {
        this.queryThreadPoolQueueCapacity = queryThreadPoolQueueCapacity;
    }

    public boolean isBrokerTopicEnable() {
        return brokerTopicEnable;
    }

    public void setBrokerTopicEnable(boolean brokerTopicEnable) {
        this.brokerTopicEnable = brokerTopicEnable;
    }

    public int getFilterServerNums() {
        return filterServerNums;
    }

    public void setFilterServerNums(int filterServerNums) {
        this.filterServerNums = filterServerNums;
    }

    public boolean isLongPollingEnable() {
        return longPollingEnable;
    }

    public void setLongPollingEnable(boolean longPollingEnable) {
        this.longPollingEnable = longPollingEnable;
    }

    public boolean isNotifyConsumerIdsChangedEnable() {
        return notifyConsumerIdsChangedEnable;
    }

    public void setNotifyConsumerIdsChangedEnable(boolean notifyConsumerIdsChangedEnable) {
        this.notifyConsumerIdsChangedEnable = notifyConsumerIdsChangedEnable;
    }

    public long getShortPollingTimeMills() {
        return shortPollingTimeMills;
    }

    public void setShortPollingTimeMills(long shortPollingTimeMills) {
        this.shortPollingTimeMills = shortPollingTimeMills;
    }

    public int getClientManageThreadPoolNums() {
        return clientManageThreadPoolNums;
    }

    public void setClientManageThreadPoolNums(int clientManageThreadPoolNums) {
        this.clientManageThreadPoolNums = clientManageThreadPoolNums;
    }

    public boolean isCommercialEnable() {
        return commercialEnable;
    }

    public void setCommercialEnable(final boolean commercialEnable) {
        this.commercialEnable = commercialEnable;
    }

    public int getCommercialTimerCount() {
        return commercialTimerCount;
    }

    public void setCommercialTimerCount(final int commercialTimerCount) {
        this.commercialTimerCount = commercialTimerCount;
    }

    public int getCommercialTransCount() {
        return commercialTransCount;
    }

    public void setCommercialTransCount(final int commercialTransCount) {
        this.commercialTransCount = commercialTransCount;
    }

    public int getCommercialBigCount() {
        return commercialBigCount;
    }

    public void setCommercialBigCount(final int commercialBigCount) {
        this.commercialBigCount = commercialBigCount;
    }

    public int getMaxDelayTime() {
        return maxDelayTime;
    }

    public void setMaxDelayTime(final int maxDelayTime) {
        this.maxDelayTime = maxDelayTime;
    }

    public int getClientManagerThreadPoolQueueCapacity() {
        return clientManagerThreadPoolQueueCapacity;
    }

    public void setClientManagerThreadPoolQueueCapacity(int clientManagerThreadPoolQueueCapacity) {
        this.clientManagerThreadPoolQueueCapacity = clientManagerThreadPoolQueueCapacity;
    }

    public int getConsumerManagerThreadPoolQueueCapacity() {
        return consumerManagerThreadPoolQueueCapacity;
    }

    public void setConsumerManagerThreadPoolQueueCapacity(int consumerManagerThreadPoolQueueCapacity) {
        this.consumerManagerThreadPoolQueueCapacity = consumerManagerThreadPoolQueueCapacity;
    }

    public int getConsumerManageThreadPoolNums() {
        return consumerManageThreadPoolNums;
    }

    public void setConsumerManageThreadPoolNums(int consumerManageThreadPoolNums) {
        this.consumerManageThreadPoolNums = consumerManageThreadPoolNums;
    }

    public int getCommercialBaseCount() {
        return commercialBaseCount;
    }

    public void setCommercialBaseCount(int commercialBaseCount) {
        this.commercialBaseCount = commercialBaseCount;
    }

    public boolean isEnableCalcFilterBitMap() {
        return enableCalcFilterBitMap;
    }

    public void setEnableCalcFilterBitMap(boolean enableCalcFilterBitMap) {
        this.enableCalcFilterBitMap = enableCalcFilterBitMap;
    }

    public int getExpectConsumerNumUseFilter() {
        return expectConsumerNumUseFilter;
    }

    public void setExpectConsumerNumUseFilter(int expectConsumerNumUseFilter) {
        this.expectConsumerNumUseFilter = expectConsumerNumUseFilter;
    }

    public int getMaxErrorRateOfBloomFilter() {
        return maxErrorRateOfBloomFilter;
    }

    public void setMaxErrorRateOfBloomFilter(int maxErrorRateOfBloomFilter) {
        this.maxErrorRateOfBloomFilter = maxErrorRateOfBloomFilter;
    }

    public long getFilterDataCleanTimeSpan() {
        return filterDataCleanTimeSpan;
    }

    public void setFilterDataCleanTimeSpan(long filterDataCleanTimeSpan) {
        this.filterDataCleanTimeSpan = filterDataCleanTimeSpan;
    }

    public boolean isFilterSupportRetry() {
        return filterSupportRetry;
    }

    public void setFilterSupportRetry(boolean filterSupportRetry) {
        this.filterSupportRetry = filterSupportRetry;
    }

    public boolean isEnablePropertyFilter() {
        return enablePropertyFilter;
    }

    public void setEnablePropertyFilter(boolean enablePropertyFilter) {
        this.enablePropertyFilter = enablePropertyFilter;
    }

    public boolean isCompressedRegister() {
        return compressedRegister;
    }

    public void setCompressedRegister(boolean compressedRegister) {
        this.compressedRegister = compressedRegister;
    }

    public boolean isForceRegister() {
        return forceRegister;
    }

    public void setForceRegister(boolean forceRegister) {
        this.forceRegister = forceRegister;
    }

    public int getHeartbeatThreadPoolQueueCapacity() {
        return heartbeatThreadPoolQueueCapacity;
    }

    public void setHeartbeatThreadPoolQueueCapacity(int heartbeatThreadPoolQueueCapacity) {
        this.heartbeatThreadPoolQueueCapacity = heartbeatThreadPoolQueueCapacity;
    }

    public int getHeartbeatThreadPoolNums() {
        return heartbeatThreadPoolNums;
    }

    public void setHeartbeatThreadPoolNums(int heartbeatThreadPoolNums) {
        this.heartbeatThreadPoolNums = heartbeatThreadPoolNums;
    }

    public long getWaitTimeMillsInHeartbeatQueue() {
        return waitTimeMillsInHeartbeatQueue;
    }

    public void setWaitTimeMillsInHeartbeatQueue(long waitTimeMillsInHeartbeatQueue) {
        this.waitTimeMillsInHeartbeatQueue = waitTimeMillsInHeartbeatQueue;
    }

    public int getRegisterNameServerPeriod() {
        return registerNameServerPeriod;
    }

    public void setRegisterNameServerPeriod(int registerNameServerPeriod) {
        this.registerNameServerPeriod = registerNameServerPeriod;
    }

    public long getTransactionTimeOut() {
        return transactionTimeOut;
    }

    public void setTransactionTimeOut(long transactionTimeOut) {
        this.transactionTimeOut = transactionTimeOut;
    }

    public int getTransactionCheckMax() {
        return transactionCheckMax;
    }

    public void setTransactionCheckMax(int transactionCheckMax) {
        this.transactionCheckMax = transactionCheckMax;
    }

    public long getTransactionCheckInterval() {
        return transactionCheckInterval;
    }

    public void setTransactionCheckInterval(long transactionCheckInterval) {
        this.transactionCheckInterval = transactionCheckInterval;
    }
}

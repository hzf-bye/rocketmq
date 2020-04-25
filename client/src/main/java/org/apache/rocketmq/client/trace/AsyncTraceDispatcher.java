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
package org.apache.rocketmq.client.trace;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.AccessChannel;
import org.apache.rocketmq.client.common.ThreadLocalIndex;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl;
import org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.RPCHook;

import static org.apache.rocketmq.client.trace.TraceConstants.TRACE_INSTANCE_NAME;

/**
 * 异步转发消息轨迹数据
 */
public class AsyncTraceDispatcher implements TraceDispatcher {

    private final static InternalLogger log = ClientLogger.getLog();
    /**
     * 异步转发，队列长度，默认为2048，当前版本不能修改。
     */
    private final int queueSize;
    /**
     * 批量消息条数，消息轨迹一次消息发送请求包含的数据条数，默认为100，当前版本不能修改。
     */
    private final int batchSize;
    /**
     * 消息轨迹一次发送的最大消息大小，默认为128K，当前版本不能修改。
     */
    private final int maxMsgSize;
    /**
     * 用来发送消息轨迹的消息发送者。
     */
    private final DefaultMQProducer traceProducer;
    /**
     * 线程池，用来异步执行消息发送。
     */
    private final ThreadPoolExecutor traceExecutor;
    /**
     * The last discard number of log
     * 记录丢弃的消息个数。
     * 整个运行过程中，丢弃的消息轨迹数据，这里要说明一点的是，如果消息TPS发送过大，异步转发线程处理不过来时，会主动丢弃消息轨迹数据。
     */
    private AtomicLong discardCount;
    /**
     * woker线程，主要负责从追加队列中获取一批待发送的消息轨迹数据，提交到线程池中执行。
     */
    private Thread worker;
    /**
     * 消息轨迹TraceContext队列，用来存放待发送到服务端的消息。
     * 客户端(消息发送、消息消费者)在收到处理结果后，将消息轨迹提交到此队列中，则会立即返回。
     * @see AsyncTraceDispatcher#append(java.lang.Object)
     * @see AsyncRunnable#run()
     */
    private ArrayBlockingQueue<TraceContext> traceContextQueue;
    /**
     * 线程池内部队列，默认长度1024。
     */
    private ArrayBlockingQueue<Runnable> appenderQueue;
    private volatile Thread shutDownHook;
    private volatile boolean stopped = false;
    /**
     * 生产者信息，记录消息发送时的轨迹信息。
     */
    private DefaultMQProducerImpl hostProducer;
    /**
     * 消费者信息，记录消息消费时的轨迹信息。
     */
    private DefaultMQPushConsumerImpl hostConsumer;
    private volatile ThreadLocalIndex sendWhichQueue = new ThreadLocalIndex();
    private String dispatcherId = UUID.randomUUID().toString();
    /**
     * 用于跟踪消息轨迹的topic名称。
     * @see MixAll#RMQ_SYS_TRACE_TOPIC 默认值
     */
    private String traceTopicName;
    private AtomicBoolean isStarted = new AtomicBoolean(false);
    private AccessChannel accessChannel = AccessChannel.LOCAL;

    public AsyncTraceDispatcher(String traceTopicName, RPCHook rpcHook) {
        // queueSize is greater than or equal to the n power of 2 of value
        this.queueSize = 2048;
        this.batchSize = 100;
        this.maxMsgSize = 128000;
        this.discardCount = new AtomicLong(0L);
        this.traceContextQueue = new ArrayBlockingQueue<TraceContext>(1024);
        this.appenderQueue = new ArrayBlockingQueue<Runnable>(queueSize);
        if (!UtilAll.isBlank(traceTopicName)) {
            this.traceTopicName = traceTopicName;
        } else {
            this.traceTopicName = MixAll.RMQ_SYS_TRACE_TOPIC;
        }
        this.traceExecutor = new ThreadPoolExecutor(//
            10, //
            20, //
            1000 * 60, //
            TimeUnit.MILLISECONDS, //
            this.appenderQueue, //
            new ThreadFactoryImpl("MQTraceSendThread_"));
        //创建用于发送消息轨迹的Producer(消息发送者)
        traceProducer = getAndCreateTraceProducer(rpcHook);
    }

    public AccessChannel getAccessChannel() {
        return accessChannel;
    }

    public void setAccessChannel(AccessChannel accessChannel) {
        this.accessChannel = accessChannel;
    }

    public String getTraceTopicName() {
        return traceTopicName;
    }

    public void setTraceTopicName(String traceTopicName) {
        this.traceTopicName = traceTopicName;
    }

    public DefaultMQProducer getTraceProducer() {
        return traceProducer;
    }

    public DefaultMQProducerImpl getHostProducer() {
        return hostProducer;
    }

    public void setHostProducer(DefaultMQProducerImpl hostProducer) {
        this.hostProducer = hostProducer;
    }

    public DefaultMQPushConsumerImpl getHostConsumer() {
        return hostConsumer;
    }

    public void setHostConsumer(DefaultMQPushConsumerImpl hostConsumer) {
        this.hostConsumer = hostConsumer;
    }

    /**
     * 开始启动，其调用的时机为启动DefaultMQProducer时，如果启用跟踪消息轨迹，则调用之。
     */
    public void start(String nameSrvAddr, AccessChannel accessChannel) throws MQClientException {
        //如果用于发送消息轨迹的发送者没有启动，则设置nameserver地址，并启动。
        if (isStarted.compareAndSet(false, true)) {
            traceProducer.setNamesrvAddr(nameSrvAddr);
            traceProducer.setInstanceName(TRACE_INSTANCE_NAME + "_" + nameSrvAddr);
            traceProducer.start();
        }
        this.accessChannel = accessChannel;
        //启动一个线程，用于执行AsyncRunnable任务
        this.worker = new Thread(new AsyncRunnable(), "MQ-AsyncTraceDispatcher-Thread-" + dispatcherId);
        this.worker.setDaemon(true);
        this.worker.start();
        this.registerShutDownHook();
    }

    /**
     * 如果还未建立发送者，则创建用于发送消息轨迹的消息发送者，
     * 其GroupName为：_INNER_TRACE_PRODUCER，消息发送超时时间5s，最大允许发送消息大小118K。
     */
    private DefaultMQProducer getAndCreateTraceProducer(RPCHook rpcHook) {
        DefaultMQProducer traceProducerInstance = this.traceProducer;
        if (traceProducerInstance == null) {
            traceProducerInstance = new DefaultMQProducer(rpcHook);
            traceProducerInstance.setProducerGroup(TraceConstants.GROUP_NAME);
            traceProducerInstance.setSendMsgTimeout(5000);
            traceProducerInstance.setVipChannelEnabled(false);
            // The max size of message is 128K
            traceProducerInstance.setMaxMessageSize(maxMsgSize - 10 * 1000);
        }
        return traceProducerInstance;
    }

    @Override
    public boolean append(final Object ctx) {
        //这里一个非常关键的点是offer方法的使用，当队列无法容纳新的元素时会立即返回false，并不会阻塞。
        boolean result = traceContextQueue.offer((TraceContext) ctx);
        if (!result) {
            log.info("buffer full" + discardCount.incrementAndGet() + " ,context is " + ctx);
        }
        return result;
    }

    @Override
    public void flush() throws IOException {
        // The maximum waiting time for refresh,avoid being written all the time, resulting in failure to return.
        long end = System.currentTimeMillis() + 500;
        while (traceContextQueue.size() > 0 || appenderQueue.size() > 0 && System.currentTimeMillis() <= end) {
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                break;
            }
        }
        log.info("------end trace send " + traceContextQueue.size() + "   " + appenderQueue.size());
    }

    @Override
    public void shutdown() {
        this.stopped = true;
        this.traceExecutor.shutdown();
        if (isStarted.get()) {
            traceProducer.shutdown();
        }
        this.removeShutdownHook();
    }

    public void registerShutDownHook() {
        if (shutDownHook == null) {
            shutDownHook = new Thread(new Runnable() {
                private volatile boolean hasShutdown = false;

                @Override
                public void run() {
                    synchronized (this) {
                        if (!this.hasShutdown) {
                            try {
                                flush();
                            } catch (IOException e) {
                                log.error("system MQTrace hook shutdown failed ,maybe loss some trace data");
                            }
                        }
                    }
                }
            }, "ShutdownHookMQTrace");
            Runtime.getRuntime().addShutdownHook(shutDownHook);
        }
    }

    public void removeShutdownHook() {
        if (shutDownHook != null) {
            Runtime.getRuntime().removeShutdownHook(shutDownHook);
        }
    }

    class AsyncRunnable implements Runnable {
        private boolean stopped;

        @Override
        public void run() {
            while (!stopped) {
                //构建待提交消息跟踪Bean，每次最多发送batchSize，默认为100条。
                List<TraceContext> contexts = new ArrayList<TraceContext>(batchSize);
                for (int i = 0; i < batchSize; i++) {
                    TraceContext context = null;
                    try {
                        //get trace data element from blocking Queue — traceContextQueue
                        //从traceContextQueue中取出一个待提交的TraceContext，设置超时时间为5s，即如何该队列中没有待提交的TraceContext，则最多等待5s。
                        context = traceContextQueue.poll(5, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                    }
                    if (context != null) {
                        contexts.add(context);
                    } else {
                        break;
                    }
                }
                if (contexts.size() > 0) {
                    //向线程池中提交任务AsyncAppenderRequest。
                    AsyncAppenderRequest request = new AsyncAppenderRequest(contexts);
                    traceExecutor.submit(request);
                } else if (AsyncTraceDispatcher.this.stopped) {
                    this.stopped = true;
                }
            }

        }
    }

    class AsyncAppenderRequest implements Runnable {
        List<TraceContext> contextList;

        public AsyncAppenderRequest(final List<TraceContext> contextList) {
            if (contextList != null) {
                this.contextList = contextList;
            } else {
                this.contextList = new ArrayList<TraceContext>(1);
            }
        }

        @Override
        public void run() {
            sendTraceData(contextList);
        }

        public void sendTraceData(List<TraceContext> contextList) {
            Map<String, List<TraceTransferBean>> transBeanMap = new HashMap<String, List<TraceTransferBean>>();
            //遍历收集的消息轨迹数据。
            for (TraceContext context : contextList) {
                if (context.getTraceBeans().isEmpty()) {
                    continue;
                }
                // Topic value corresponding to original message entity content
                //获取存储消息轨迹的Topic。
                String topic = context.getTraceBeans().get(0).getTopic();
                String regionId = context.getRegionId();
                // Use  original message entity's topic as key
                String key = topic;
                if (!StringUtils.isBlank(regionId)) {
                    key = key + TraceConstants.CONTENT_SPLITOR + regionId;
                }
                List<TraceTransferBean> transBeanList = transBeanMap.get(key);
                if (transBeanList == null) {
                    transBeanList = new ArrayList<TraceTransferBean>();
                    transBeanMap.put(key, transBeanList);
                }
                //对TraceContext进行编码，
                TraceTransferBean traceData = TraceDataEncoder.encoderFromContextBean(context);
                transBeanList.add(traceData);
            }
            for (Map.Entry<String, List<TraceTransferBean>> entry : transBeanMap.entrySet()) {
                String[] key = entry.getKey().split(String.valueOf(TraceConstants.CONTENT_SPLITOR));
                String dataTopic = entry.getKey();
                String regionId = null;
                if (key.length > 1) {
                    dataTopic = key[0];
                    regionId = key[1];
                }
                //将编码后的数据发送到Broker服务器。
                flushData(entry.getValue(), dataTopic, regionId);
            }
        }

        /**
         * Batch sending data actually
         */
        private void flushData(List<TraceTransferBean> transBeanList, String dataTopic, String regionId) {
            if (transBeanList.size() == 0) {
                return;
            }
            // Temporary buffer
            StringBuilder buffer = new StringBuilder(1024);
            int count = 0;
            Set<String> keySet = new HashSet<String>();

            for (TraceTransferBean bean : transBeanList) {
                // Keyset of message trace includes msgId of or original message
                keySet.addAll(bean.getTransKey());
                buffer.append(bean.getTransData());
                count++;
                // Ensure that the size of the package should not exceed the upper limit.
                if (buffer.length() >= traceProducer.getMaxMessageSize()) {
                    sendTraceDataByMQ(keySet, buffer.toString(), dataTopic, regionId);
                    // Clear temporary buffer after finishing
                    buffer.delete(0, buffer.length());
                    keySet.clear();
                    count = 0;
                }
            }
            if (count > 0) {
                sendTraceDataByMQ(keySet, buffer.toString(), dataTopic, regionId);
            }
            transBeanList.clear();
        }

        /**
         * Send message trace data
         *
         * @param keySet the keyset in this batch(including msgId in original message not offsetMsgId)
         * @param data the message trace data in this batch
         */
        private void sendTraceDataByMQ(Set<String> keySet, final String data, String dataTopic, String regionId) {
            String traceTopic = traceTopicName;
            if (AccessChannel.CLOUD == accessChannel) {
                traceTopic = TraceConstants.TRACE_TOPIC_PREFIX + regionId;
            }
            final Message message = new Message(traceTopic, data.getBytes());
            // Keyset of message trace includes msgId of or original message
            message.setKeys(keySet);
            try {
                Set<String> traceBrokerSet = tryGetMessageQueueBrokerSet(traceProducer.getDefaultMQProducerImpl(), traceTopic);
                SendCallback callback = new SendCallback() {
                    @Override
                    public void onSuccess(SendResult sendResult) {

                    }

                    @Override
                    public void onException(Throwable e) {
                        log.info("send trace data ,the traceData is " + data);
                    }
                };
                if (traceBrokerSet.isEmpty()) {
                    // No cross set
                    traceProducer.send(message, callback, 5000);
                } else {
                    traceProducer.send(message, new MessageQueueSelector() {
                        @Override
                        public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                            Set<String> brokerSet = (Set<String>) arg;
                            List<MessageQueue> filterMqs = new ArrayList<MessageQueue>();
                            for (MessageQueue queue : mqs) {
                                if (brokerSet.contains(queue.getBrokerName())) {
                                    filterMqs.add(queue);
                                }
                            }
                            int index = sendWhichQueue.getAndIncrement();
                            int pos = Math.abs(index) % filterMqs.size();
                            if (pos < 0) {
                                pos = 0;
                            }
                            return filterMqs.get(pos);
                        }
                    }, traceBrokerSet, callback);
                }

            } catch (Exception e) {
                log.info("send trace data,the traceData is" + data);
            }
        }

        private Set<String> tryGetMessageQueueBrokerSet(DefaultMQProducerImpl producer, String topic) {
            Set<String> brokerSet = new HashSet<String>();
            TopicPublishInfo topicPublishInfo = producer.getTopicPublishInfoTable().get(topic);
            if (null == topicPublishInfo || !topicPublishInfo.ok()) {
                producer.getTopicPublishInfoTable().putIfAbsent(topic, new TopicPublishInfo());
                producer.getmQClientFactory().updateTopicRouteInfoFromNameServer(topic);
                topicPublishInfo = producer.getTopicPublishInfoTable().get(topic);
            }
            if (topicPublishInfo.isHaveTopicRouterInfo() || topicPublishInfo.ok()) {
                for (MessageQueue queue : topicPublishInfo.getMessageQueueList()) {
                    brokerSet.add(queue.getBrokerName());
                }
            }
            return brokerSet;
        }
    }

}

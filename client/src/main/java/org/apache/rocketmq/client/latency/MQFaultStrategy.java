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

package org.apache.rocketmq.client.latency;

import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageQueue;

/**
 * 消息失败策略，延迟实现的门面类
 */
public class MQFaultStrategy {
    private final static InternalLogger log = ClientLogger.getLog();
    private final LatencyFaultTolerance<String> latencyFaultTolerance = new LatencyFaultToleranceImpl();

    /**
     * 是否启用Broker故障延迟机制
     * false-不启用
     * true-启用
     * @see MQFaultStrategy#selectOneMessageQueue(org.apache.rocketmq.client.impl.producer.TopicPublishInfo, java.lang.String)
     */
    private boolean sendLatencyFaultEnable = false;

    /**
     * 根据{@link LatencyFaultToleranceImpl.FaultItem#currentLatency}本次消息发送延迟，
     * 从latencyMax尾部向前找到第一个比currentLatency小的索引的index，如果没有找到返回0。
     * 然后根据这个索引从notAvailableDuration数组中取出对应的时间，在这个时间内，Broker将被设置为不可用。
     */
    private long[] latencyMax = {50L, 100L, 550L, 1000L, 2000L, 3000L, 15000L};
    private long[] notAvailableDuration = {0L, 0L, 30000L, 60000L, 120000L, 180000L, 600000L};

    public long[] getNotAvailableDuration() {
        return notAvailableDuration;
    }

    public void setNotAvailableDuration(final long[] notAvailableDuration) {
        this.notAvailableDuration = notAvailableDuration;
    }

    public long[] getLatencyMax() {
        return latencyMax;
    }

    public void setLatencyMax(final long[] latencyMax) {
        this.latencyMax = latencyMax;
    }

    public boolean isSendLatencyFaultEnable() {
        return sendLatencyFaultEnable;
    }

    public void setSendLatencyFaultEnable(final boolean sendLatencyFaultEnable) {
        this.sendLatencyFaultEnable = sendLatencyFaultEnable;
    }

    /**
     * lastBrokerName就是上一次选择执行发送消息失败的Broker，第一次执行是为null
     */
    public MessageQueue selectOneMessageQueue(final TopicPublishInfo tpInfo, final String lastBrokerName) {
        //是否启用Broker故障延迟机制
        //此机制如果在某一次发送消息后Broker故障，那么在后续能自动将此Broker排除在消息队列选择外，避免不必要的开销。
        /*
         * 那么为什么Broke故障后路由信息中还会有此Broker的路由信息呢？
         * 1.NameServer检测Broker是否可用是延迟的，最短为10s一次的心跳检测。
         * 2.NameSrver不会检测到Broker宕机后立马推送给生产者，而是消息生产者每隔30s更新一次路由信息，
         * 所以生产者最快感知Broker最新的路由信息要30s
         */
        if (this.sendLatencyFaultEnable) {
            //启用
            try {
                //获取自增随机值
                int index = tpInfo.getSendWhichQueue().getAndIncrement();
                for (int i = 0; i < tpInfo.getMessageQueueList().size(); i++) {
                    int pos = Math.abs(index++) % tpInfo.getMessageQueueList().size();
                    if (pos < 0)
                        pos = 0;
                    //获取一个随机的消息队列
                    MessageQueue mq = tpInfo.getMessageQueueList().get(pos);
                    //验证该消息队列是否可用
                    //如果该Broker不在faultItemTable中直接返回true。
                    if (latencyFaultTolerance.isAvailable(mq.getBrokerName())) {
                        /*
                         * 在某次发消息中
                         * 1.如果是第一次选择，null == lastBrokerName，那么选中此Broker中的队列那么直接返回
                         * 2.如果不是第一次选择那么lastBrokerName!=null,那么如果再次选中lastBrokerName说明此Broker刚才故障此时已经恢复，直接返回
                         * 3.如果不是第一次选择那么lastBrokerName!=null,那么如果再次选中的Broker不是lastBrokerName则不返回。
                         *      原因可能是当前选中的Broker存在故障的可能？因此不选择？
                         */
                        if (null == lastBrokerName || mq.getBrokerName().equals(lastBrokerName))
                            return mq;
                    }
                }

                //尝试从规避的Broker中选择一个可用的Broker，如果没有找到，将返回null。
                //此方法选中的Broker优先规则是broker可用的，消息发送延迟小的，故障规避开始时间小的。
                final String notBestBroker = latencyFaultTolerance.pickOneAtLeast();
                //找到此Broker的写队列数
                int writeQueueNums = tpInfo.getQueueIdByBroker(notBestBroker);
                //如果此Broker存在写队列
                if (writeQueueNums > 0) {
                    //随机选择一个Broker
                    final MessageQueue mq = tpInfo.selectOneMessageQueue();
                    if (notBestBroker != null) {
                        //选中了一个相对好的Broker后直接使用此Broker，以及随机一个队列
                        mq.setBrokerName(notBestBroker);
                        mq.setQueueId(tpInfo.getSendWhichQueue().getAndIncrement() % writeQueueNums);
                    }
                    return mq;
                } else {
                    latencyFaultTolerance.remove(notBestBroker);
                }
            } catch (Exception e) {
                log.error("Error occurred when selecting message queue", e);
            }

            return tpInfo.selectOneMessageQueue();
        }

        //不启用，默认机制
        /*
         * 此机制在某一次消息的发送后面的重试逻辑中，能规避故障的Broker,因为再某次失败重试的过程中有lastBrokerName会规避故障的Broker
         * 但是如果Broker宕机，由于messageQueueList是按照Broker排序的，
         * 如果上一次发消息时选择是宕机的Broker的第一个队列，那么随后一次发消息（独立的另一个消息）选择的可能就是宕机的Broker的第二个队列
         * 那么第一次发送消息还是会发送失败，
         * 再次引发重试才能规避此Broker，带来不必要的性能损耗。
         *
         *
         */
        return tpInfo.selectOneMessageQueue(lastBrokerName);
    }

    /**
     * 更新失败broker条目
     * @param brokerName brokerName
     * @param currentLatency 本次消息发送延迟时间
     * @param isolation 是否隔离 true-使用默认时长30s来计算broker故障规避时长；false-则使用本次消息发送延迟时间来计算Broker故障规避时长
     */
    public void updateFaultItem(final String brokerName, final long currentLatency, boolean isolation) {
        if (this.sendLatencyFaultEnable) {
            long duration = computeNotAvailableDuration(isolation ? 30000 : currentLatency);
            this.latencyFaultTolerance.updateFaultItem(brokerName, currentLatency, duration);
        }
    }

    /**
     * 计算因本次消息发送故障需要将Broker规避的时长，
     * 也就是接下来多久的时间内该Broker将不参与消息发送队列负载。
     * 具体算法：从latencyMax尾部向前找到第一个比currentLatency小的索引的index，如果没有找到返回0。
     * 然后根据这个索引从notAvailableDuration数组中取出对应的时间。
     */
    private long computeNotAvailableDuration(final long currentLatency) {
        for (int i = latencyMax.length - 1; i >= 0; i--) {
            if (currentLatency >= latencyMax[i])
                return this.notAvailableDuration[i];
        }

        return 0;
    }
}

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

package org.apache.rocketmq.common.subscription;

import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MixAll;

/**
 * 消费组配置信息
 * 配置信息的使用
 * @see org.apache.rocketmq.broker.processor.PullMessageProcessor#processRequest(io.netty.channel.Channel, org.apache.rocketmq.remoting.protocol.RemotingCommand, boolean)
 */
public class SubscriptionGroupConfig {

    /**
     * 消费组名
     */
    private String groupName;

    /**
     * 当前消费组是否允许消费消息，默认为true
     *
     * 如果为false，该消费组无法拉取消息，从而无法消费消息。
     */
    private boolean consumeEnable = true;

    /**
     * 是否允许队列最小偏移量开始消费，目前未使用该参数
     */
    private boolean consumeFromMinEnable = true;

    /**
     * 该消费组是否能已广播模式消费，如果设置为false，则标识只能以集群模式消费。
     */
    private boolean consumeBroadcastEnable = true;

    /**
     * 重试队列个数，默认为1
     * 每个Broker上一个重试队列
     */
    private int retryQueueNums = 1;

    /**
     * 消息最大重试次数
     */
    private int retryMaxTimes = 16;

    /**
     * 默认主Broker
     */
    private long brokerId = MixAll.MASTER_ID;

    /**
     * 当Broker建议下一次从Broker从节点拉取消息时，
     * whichBrokerWhenConsumeSlowly指定从哪个从节点拉取
     * 默认值1，即表示从brokerId=1的从节点拉取
     *
     * 当一个master有多台salva服务器，那么参与消息负载拉取的从服务器指挥室其中一个。
     * 设置
     * @see org.apache.rocketmq.tools.command.consumer.UpdateSubGroupSubCommand#execute(org.apache.commons.cli.CommandLine, org.apache.commons.cli.Options, org.apache.rocketmq.remoting.RPCHook)
     * 可以通过mqadmin命令修改
     */
    private long whichBrokerWhenConsumeSlowly = 1;

    /**
     * 当有新的消费者或者消费者订阅topic发生变化时是否立即进行消息队列重新负载
     * @see org.apache.rocketmq.broker.client.ConsumerManager#registerConsumer(java.lang.String, org.apache.rocketmq.broker.client.ClientChannelInfo, org.apache.rocketmq.common.protocol.heartbeat.ConsumeType, org.apache.rocketmq.common.protocol.heartbeat.MessageModel, org.apache.rocketmq.common.consumer.ConsumeFromWhere, java.util.Set, boolean)
     * 消费者订阅信息配置存储在Broker的${ROCKET_HOME}/store/config/subscriptionGroup.json。
     * 默认情况下{@link BrokerConfig#autoCreateSubscriptionGroup}为true，表示在第一次使用消费组
     * 配置信息时如果不存在，则使用自动创建一个{@link org.apache.rocketmq.broker.subscription.SubscriptionGroupManager#findSubscriptionGroupConfig(java.lang.String)}，
     * 如果为false,则只能通过客户端命令
     * mqadmin updateSubGroup创建后修改相关参数。
     */
    private boolean notifyConsumerIdsChangedEnable = true;

    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    public boolean isConsumeEnable() {
        return consumeEnable;
    }

    public void setConsumeEnable(boolean consumeEnable) {
        this.consumeEnable = consumeEnable;
    }

    public boolean isConsumeFromMinEnable() {
        return consumeFromMinEnable;
    }

    public void setConsumeFromMinEnable(boolean consumeFromMinEnable) {
        this.consumeFromMinEnable = consumeFromMinEnable;
    }

    public boolean isConsumeBroadcastEnable() {
        return consumeBroadcastEnable;
    }

    public void setConsumeBroadcastEnable(boolean consumeBroadcastEnable) {
        this.consumeBroadcastEnable = consumeBroadcastEnable;
    }

    public int getRetryQueueNums() {
        return retryQueueNums;
    }

    public void setRetryQueueNums(int retryQueueNums) {
        this.retryQueueNums = retryQueueNums;
    }

    public int getRetryMaxTimes() {
        return retryMaxTimes;
    }

    public void setRetryMaxTimes(int retryMaxTimes) {
        this.retryMaxTimes = retryMaxTimes;
    }

    public long getBrokerId() {
        return brokerId;
    }

    public void setBrokerId(long brokerId) {
        this.brokerId = brokerId;
    }

    public long getWhichBrokerWhenConsumeSlowly() {
        return whichBrokerWhenConsumeSlowly;
    }

    public void setWhichBrokerWhenConsumeSlowly(long whichBrokerWhenConsumeSlowly) {
        this.whichBrokerWhenConsumeSlowly = whichBrokerWhenConsumeSlowly;
    }

    public boolean isNotifyConsumerIdsChangedEnable() {
        return notifyConsumerIdsChangedEnable;
    }

    public void setNotifyConsumerIdsChangedEnable(final boolean notifyConsumerIdsChangedEnable) {
        this.notifyConsumerIdsChangedEnable = notifyConsumerIdsChangedEnable;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (brokerId ^ (brokerId >>> 32));
        result = prime * result + (consumeBroadcastEnable ? 1231 : 1237);
        result = prime * result + (consumeEnable ? 1231 : 1237);
        result = prime * result + (consumeFromMinEnable ? 1231 : 1237);
        result = prime * result + (notifyConsumerIdsChangedEnable ? 1231 : 1237);
        result = prime * result + ((groupName == null) ? 0 : groupName.hashCode());
        result = prime * result + retryMaxTimes;
        result = prime * result + retryQueueNums;
        result =
            prime * result + (int) (whichBrokerWhenConsumeSlowly ^ (whichBrokerWhenConsumeSlowly >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        SubscriptionGroupConfig other = (SubscriptionGroupConfig) obj;
        if (brokerId != other.brokerId)
            return false;
        if (consumeBroadcastEnable != other.consumeBroadcastEnable)
            return false;
        if (consumeEnable != other.consumeEnable)
            return false;
        if (consumeFromMinEnable != other.consumeFromMinEnable)
            return false;
        if (groupName == null) {
            if (other.groupName != null)
                return false;
        } else if (!groupName.equals(other.groupName))
            return false;
        if (retryMaxTimes != other.retryMaxTimes)
            return false;
        if (retryQueueNums != other.retryQueueNums)
            return false;
        if (whichBrokerWhenConsumeSlowly != other.whichBrokerWhenConsumeSlowly)
            return false;
        if (notifyConsumerIdsChangedEnable != other.notifyConsumerIdsChangedEnable)
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "SubscriptionGroupConfig [groupName=" + groupName + ", consumeEnable=" + consumeEnable
            + ", consumeFromMinEnable=" + consumeFromMinEnable + ", consumeBroadcastEnable="
            + consumeBroadcastEnable + ", retryQueueNums=" + retryQueueNums + ", retryMaxTimes="
            + retryMaxTimes + ", brokerId=" + brokerId + ", whichBrokerWhenConsumeSlowly="
            + whichBrokerWhenConsumeSlowly + ", notifyConsumerIdsChangedEnable="
            + notifyConsumerIdsChangedEnable + "]";
    }
}

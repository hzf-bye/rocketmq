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

/**
 * $Id: PullMessageRequestHeader.java 1835 2013-05-16 02:00:50Z vintagewang@apache.org $
 */
package org.apache.rocketmq.common.protocol.header;

import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.common.sysflag.PullSysFlag;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.annotation.CFNullable;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

/**
 * 消费者去Broker服务器获取消息请求参数
 */
public class PullMessageRequestHeader implements CommandCustomHeader {

    /**
     * 消费组名称
     */
    @CFNotNull
    private String consumerGroup;
    /**
     * topic
     */
    @CFNotNull
    private String topic;
    /**
     * 队列id
     */
    @CFNotNull
    private Integer queueId;
    /**
     * 消息拉取偏移量
     * ConsumeQueue文件中的偏移量，相当于数组下标，乘以{@link org.apache.rocketmq.store.ConsumeQueue#CQ_STORE_UNIT_SIZE}
     * 即得到consumeQueue文件的物理偏移量
     */
    @CFNotNull
    private Long queueOffset;
    /**
     * 本次拉取最大消息条数，默认32条
     * {@link org.apache.rocketmq.client.consumer.DefaultMQPushConsumer#pullBatchSize}
     */
    @CFNotNull
    private Integer maxMsgNums;
    /**
     * 拉取系统标志
     * {@link PullSysFlag}
     */
    @CFNotNull
    private Integer sysFlag;
    /**
     * 当前MessageQueue消费进度（内存中）
     */
    @CFNotNull
    private Long commitOffset;
    /**
     * 消息拉取过程中允许Broker挂起时间
     * push模式默认15s
     * @see org.apache.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl#BROKER_SUSPEND_MAX_TIME_MILLIS
     */
    @CFNotNull
    private Long suspendTimeoutMillis;
    /**
     * 消息过滤表达式
     * @see SubscriptionData#subString
     */
    @CFNullable
    private String subscription;
    /**
     * {@link SubscriptionData#subVersion}
     */
    @CFNotNull
    private Long subVersion;
    /**
     * 消息表达式类型，分为TAG、SQL92
     * @see ExpressionType
     */
    private String expressionType;

    @Override
    public void checkFields() throws RemotingCommandException {
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public Integer getQueueId() {
        return queueId;
    }

    public void setQueueId(Integer queueId) {
        this.queueId = queueId;
    }

    public Long getQueueOffset() {
        return queueOffset;
    }

    public void setQueueOffset(Long queueOffset) {
        this.queueOffset = queueOffset;
    }

    public Integer getMaxMsgNums() {
        return maxMsgNums;
    }

    public void setMaxMsgNums(Integer maxMsgNums) {
        this.maxMsgNums = maxMsgNums;
    }

    public Integer getSysFlag() {
        return sysFlag;
    }

    public void setSysFlag(Integer sysFlag) {
        this.sysFlag = sysFlag;
    }

    public Long getCommitOffset() {
        return commitOffset;
    }

    public void setCommitOffset(Long commitOffset) {
        this.commitOffset = commitOffset;
    }

    public Long getSuspendTimeoutMillis() {
        return suspendTimeoutMillis;
    }

    public void setSuspendTimeoutMillis(Long suspendTimeoutMillis) {
        this.suspendTimeoutMillis = suspendTimeoutMillis;
    }

    public String getSubscription() {
        return subscription;
    }

    public void setSubscription(String subscription) {
        this.subscription = subscription;
    }

    public Long getSubVersion() {
        return subVersion;
    }

    public void setSubVersion(Long subVersion) {
        this.subVersion = subVersion;
    }

    public String getExpressionType() {
        return expressionType;
    }

    public void setExpressionType(String expressionType) {
        this.expressionType = expressionType;
    }
}

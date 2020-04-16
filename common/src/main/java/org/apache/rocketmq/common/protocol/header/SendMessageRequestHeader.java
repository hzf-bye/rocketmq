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
 * $Id: SendMessageRequestHeader.java 1835 2013-05-16 02:00:50Z vintagewang@apache.org $
 */
package org.apache.rocketmq.common.protocol.header;

import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.annotation.CFNullable;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

public class SendMessageRequestHeader implements CommandCustomHeader {

    /**
     * 生产者组
     */
    @CFNotNull
    private String producerGroup;
    /**
     * 主题名称
     */
    @CFNotNull
    private String topic;
    /**
     * 默认创建主题key
     * 如果在发送消息时 topic 没有被事先创建，且
     * @see BrokerConfig#autoCreateTopicEnable 为true.
     * 那么系统会自动创建此topic。且topic属性继承自defaultTopic
     * 而defaultTopic的创建是在Broker启动时创建的
     * @see org.apache.rocketmq.broker.topic.TopicConfigManager#TopicConfigManager(org.apache.rocketmq.broker.BrokerController)
     */
    @CFNotNull
    private String defaultTopic;
    /**
     * 该主体在单个Broker默认队列数
     */
    @CFNotNull
    private Integer defaultTopicQueueNums;
    /**
     * 队列id（队列序列号）
     */
    @CFNotNull
    private Integer queueId;
    /**
     * 消息系统标记
     * {@link MessageSysFlag}
     */
    @CFNotNull
    private Integer sysFlag;
    /**
     * 消息发送时间
     */
    @CFNotNull
    private Long bornTimestamp;
    /**
     * 消息标记(RocketMQ对消息中的falg不做任何处理，供应用程序使用)
     */
    @CFNotNull
    private Integer flag;
    /**
     * 消息扩展属性
     */
    @CFNullable
    private String properties;
    /**
     * 重新消费次数
     */
    @CFNullable
    private Integer reconsumeTimes;
    /**
     * 是否为单元化模式。
     */
    @CFNullable
    private boolean unitMode = false;
    @CFNullable
    /**
     * 是否是批量消息
     */
    private boolean batch = false;
    /**
     * 最大重新消费次数
     */
    private Integer maxReconsumeTimes;

    @Override
    public void checkFields() throws RemotingCommandException {
    }

    public String getProducerGroup() {
        return producerGroup;
    }

    public void setProducerGroup(String producerGroup) {
        this.producerGroup = producerGroup;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getDefaultTopic() {
        return defaultTopic;
    }

    public void setDefaultTopic(String defaultTopic) {
        this.defaultTopic = defaultTopic;
    }

    public Integer getDefaultTopicQueueNums() {
        return defaultTopicQueueNums;
    }

    public void setDefaultTopicQueueNums(Integer defaultTopicQueueNums) {
        this.defaultTopicQueueNums = defaultTopicQueueNums;
    }

    public Integer getQueueId() {
        return queueId;
    }

    public void setQueueId(Integer queueId) {
        this.queueId = queueId;
    }

    public Integer getSysFlag() {
        return sysFlag;
    }

    public void setSysFlag(Integer sysFlag) {
        this.sysFlag = sysFlag;
    }

    public Long getBornTimestamp() {
        return bornTimestamp;
    }

    public void setBornTimestamp(Long bornTimestamp) {
        this.bornTimestamp = bornTimestamp;
    }

    public Integer getFlag() {
        return flag;
    }

    public void setFlag(Integer flag) {
        this.flag = flag;
    }

    public String getProperties() {
        return properties;
    }

    public void setProperties(String properties) {
        this.properties = properties;
    }

    public Integer getReconsumeTimes() {
        return reconsumeTimes;
    }

    public void setReconsumeTimes(Integer reconsumeTimes) {
        this.reconsumeTimes = reconsumeTimes;
    }

    public boolean isUnitMode() {
        return unitMode;
    }

    public void setUnitMode(boolean isUnitMode) {
        this.unitMode = isUnitMode;
    }

    public Integer getMaxReconsumeTimes() {
        return maxReconsumeTimes;
    }

    public void setMaxReconsumeTimes(final Integer maxReconsumeTimes) {
        this.maxReconsumeTimes = maxReconsumeTimes;
    }

    public boolean isBatch() {
        return batch;
    }

    public void setBatch(boolean batch) {
        this.batch = batch;
    }
}

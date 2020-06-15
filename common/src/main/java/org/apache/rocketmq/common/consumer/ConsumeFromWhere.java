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
package org.apache.rocketmq.common.consumer;

/**
 * @see org.apache.rocketmq.client.impl.consumer.RebalancePushImpl#computePullFromWhere(org.apache.rocketmq.common.message.MessageQueue)
 * ConsumeFromWhere相关消息消费进度校验策略，只有在从Broker中获取的消费进度返回-1时才会生效，
 * 如果从Broker消息消费进度缓存中返回的消费进度小于-1，表示偏移量非法，则丢弃该消费队列等待下一次负载均衡。
 *
 */
public enum ConsumeFromWhere {

    /**
     * 从Broker消息进度文件中存储的偏移量{@link org.apache.rocketmq.broker.offset.ConsumerOffsetManager#offsetTable}开始消费，
     * 如果不存在当前消费队列的消费偏移量，则从0开始消费。
     * 从队列当前最大偏移量开始消费，即跳过历史消息
     */
    CONSUME_FROM_LAST_OFFSET,

    @Deprecated
    CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST,
    @Deprecated
    CONSUME_FROM_MIN_OFFSET,
    @Deprecated
    CONSUME_FROM_MAX_OFFSET,
    /**
     * 从队列当前最小偏移量开始消费，即历史消息（还储存在broker的）全部消费一遍
     */
    CONSUME_FROM_FIRST_OFFSET,
    /**
     * 从消费者启动时间戳开始消费
     * 和{@link org.apache.rocketmq.client.consumer.DefaultMQPushConsumer#consumeTimestamp}配合使用，默认是半个小时以前
     */
    CONSUME_FROM_TIMESTAMP,
}

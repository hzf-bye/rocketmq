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
package org.apache.rocketmq.client.consumer.listener;

import org.apache.rocketmq.client.impl.consumer.ConsumeMessageConcurrentlyService;
import org.apache.rocketmq.common.message.MessageQueue;

/**
 * Consumer concurrent consumption context
 */
public class ConsumeConcurrentlyContext {
    private final MessageQueue messageQueue;
    /**
     * Message consume retry strategy<br>
     * -1,no retry,put into DLQ directly<br>
     * 0,broker control retry frequency<br>
     * >0,client control retry frequency
     * 集群模式下消息并发消费失败时的处理策略
     * @see ConsumeMessageConcurrentlyService#sendMessageBack(org.apache.rocketmq.common.message.MessageExt, org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext)
     * -1 表示消息直接进入DLQ队列，私信队列
     * 0  表示消息延迟级别由当前消息的重试次数 + 1
     * >0 表示当前消息的延迟级别
     * @see org.apache.rocketmq.broker.processor.SendMessageProcessor#consumerSendMsgBack(io.netty.channel.ChannelHandlerContext, org.apache.rocketmq.remoting.protocol.RemotingCommand)
     * 中对于level的处理
     */
    private int delayLevelWhenNextConsume = 0;

    /**
     * 业务消息监听器返回 CONSUME_SUCCESS 时，
     * 业务端可以更改此时，以便于客户端统计消费成功的消息的数量，以及消息失败的消息的数量
     * @see ConsumeMessageConcurrentlyService#processConsumeResult(org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus, org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext, org.apache.rocketmq.client.impl.consumer.ConsumeMessageConcurrentlyService.ConsumeRequest)
     */
    private int ackIndex = Integer.MAX_VALUE;

    public ConsumeConcurrentlyContext(MessageQueue messageQueue) {
        this.messageQueue = messageQueue;
    }

    public int getDelayLevelWhenNextConsume() {
        return delayLevelWhenNextConsume;
    }

    public void setDelayLevelWhenNextConsume(int delayLevelWhenNextConsume) {
        this.delayLevelWhenNextConsume = delayLevelWhenNextConsume;
    }

    public MessageQueue getMessageQueue() {
        return messageQueue;
    }

    public int getAckIndex() {
        return ackIndex;
    }

    public void setAckIndex(int ackIndex) {
        this.ackIndex = ackIndex;
    }
}

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
 * $Id: EndTransactionRequestHeader.java 1835 2013-05-16 02:00:50Z vintagewang@apache.org $
 */
package org.apache.rocketmq.common.protocol.header;

import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

public class CheckTransactionStateRequestHeader implements CommandCustomHeader {
    /**
     * 消息在consumeQueue中的偏移量
     */
    @CFNotNull
    private Long tranStateTableOffset;
    /**
     * 消息物理偏移量
     */
    @CFNotNull
    private Long commitLogOffset;
    /**
     * 消息唯一id，此消息在消息生成者客户端生成
     * @see MessageClientIDSetter#setUniqID(org.apache.rocketmq.common.message.Message)
     * @see DefaultMQProducerImpl#sendKernelImpl(org.apache.rocketmq.common.message.Message, org.apache.rocketmq.common.message.MessageQueue, org.apache.rocketmq.client.impl.CommunicationMode, org.apache.rocketmq.client.producer.SendCallback, org.apache.rocketmq.client.impl.producer.TopicPublishInfo, long)
     */
    private String msgId;
    /**
     * 事务消息id 同msgId
     */
    private String transactionId;
    /**
     * 消息id，此消息id记录了消息所在broker的ip+port，以及消息的物理偏移量。
     * @see MessageDecoder#createMessageId(java.nio.ByteBuffer, java.nio.ByteBuffer, long)
     */
    private String offsetMsgId;

    @Override
    public void checkFields() throws RemotingCommandException {
    }

    public Long getTranStateTableOffset() {
        return tranStateTableOffset;
    }

    public void setTranStateTableOffset(Long tranStateTableOffset) {
        this.tranStateTableOffset = tranStateTableOffset;
    }

    public Long getCommitLogOffset() {
        return commitLogOffset;
    }

    public void setCommitLogOffset(Long commitLogOffset) {
        this.commitLogOffset = commitLogOffset;
    }

    public String getMsgId() {
        return msgId;
    }

    public void setMsgId(String msgId) {
        this.msgId = msgId;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public String getOffsetMsgId() {
        return offsetMsgId;
    }

    public void setOffsetMsgId(String offsetMsgId) {
        this.offsetMsgId = offsetMsgId;
    }
}

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
package org.apache.rocketmq.client.consumer.rebalance;

import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageQueue;

/**
 * Average Hashing queue algorithm
 * 平均分配算法
 * 比如 现在有8个消息消费队列q0,q1,q2,q3,q4,q5,q6,q7；三个消费组c0,c1,c2
 * 消息队列分配如下
 * c1:q0,q1,q2
 * c2:q3,q4,q5
 * c3:q6,q7
 * 这也是RocketMq默认的分配算法
 */
public class AllocateMessageQueueAveragely implements AllocateMessageQueueStrategy {
    private final InternalLogger log = ClientLogger.getLog();

    /**
     *
     * @param consumerGroup current consumer group 当前消费组名
     * @param currentCID current consumer id 当前客户端id
     * @param mqAll message queue set in current topic 消费组下的当前topic对应的所有消费队列信息
     * @param cidAll consumer set in current consumer group 消费组下的所有客户端id，从Broker中获取
     */
    @Override
    public List<MessageQueue> allocate(String consumerGroup, String currentCID, List<MessageQueue> mqAll,
        List<String> cidAll) {
        if (currentCID == null || currentCID.length() < 1) {
            throw new IllegalArgumentException("currentCID is empty");
        }
        if (mqAll == null || mqAll.isEmpty()) {
            throw new IllegalArgumentException("mqAll is null or mqAll empty");
        }
        if (cidAll == null || cidAll.isEmpty()) {
            throw new IllegalArgumentException("cidAll is null or cidAll empty");
        }

        List<MessageQueue> result = new ArrayList<MessageQueue>();
        if (!cidAll.contains(currentCID)) {
            log.info("[BUG] ConsumerGroup: {} The consumerId: {} not in cidAll: {}",
                consumerGroup,
                currentCID,
                cidAll);
            return result;
        }

        //获取当前客户端id，在cidAll中的下标
        int index = cidAll.indexOf(currentCID);
        //取模，
        int mod = mqAll.size() % cidAll.size();
        // 获取当前客户端可能分配到的消费队列数量。
        int averageSize =
            mqAll.size() <= cidAll.size() ? 1 : (mod > 0 && index < mod ? mqAll.size() / cidAll.size()
                + 1 : mqAll.size() / cidAll.size());
        //获取起始位置
        int startIndex = (mod > 0 && index < mod) ? index * averageSize : index * averageSize + mod;
        //计算当前客户端分配到的队列数量
        //如果消费队列数量小于客户端数量，那么此时计算出后面的客户端的range <= 0即mqAll.size() - startIndex <= 0 表示有些客户端无法消费消息。
        int range = Math.min(averageSize, mqAll.size() - startIndex);
        for (int i = 0; i < range; i++) {
            result.add(mqAll.get((startIndex + i) % mqAll.size()));
        }
        return result;
    }

    @Override
    public String getName() {
        return "AVG";
    }
}

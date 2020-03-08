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
package org.apache.rocketmq.client.impl.producer;

import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.client.common.ThreadLocalIndex;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.route.QueueData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;

public class TopicPublishInfo {
    /**
     * 是否是顺序消息
     */
    private boolean orderTopic = false;

    /**
     * 是否找到主题的路由信息
     * 即为true时 messageQueueList与topicRouteData不为空
     */
    private boolean haveTopicRouterInfo = false;
    /**
     * 该主题队列的消息队列
     *
     * 例如topicA在broker-a与broker-b上分别创建了4个队列
     * [{"brokerName":"broker-a","queueId":0,"topic":"topicA"},{"brokerName":"broker-a","queueId":1,"topic":"topicA"},{"brokerName":"broker-a","queueId":2,"topic":"topicA"},{"brokerName":"broker-a","queueId":3,"topic":"topicA"},{"brokerName":"broker-b","queueId":0,"topic":"topicA"},{"brokerName":"broker-b","queueId":1,"topic":"topicA"},{"brokerName":"broker-b","queueId":2,"topic":"topicA"},{"brokerName":"broker-b","queueId":3,"topic":"topicA"}]
     *
     */
    private List<MessageQueue> messageQueueList = new ArrayList<MessageQueue>();
    /**
     * 没选择一次消息队列，该值会自增1，如果大于Integer.MAX_VALUE则重置为0，用于选择消息队列。
     * 负责均衡策略的索引
     */
    private volatile ThreadLocalIndex sendWhichQueue = new ThreadLocalIndex();
    /**
     * topic路由信息
     */
    private TopicRouteData topicRouteData;

    public boolean isOrderTopic() {
        return orderTopic;
    }

    public void setOrderTopic(boolean orderTopic) {
        this.orderTopic = orderTopic;
    }

    public boolean ok() {
        return null != this.messageQueueList && !this.messageQueueList.isEmpty();
    }

    public List<MessageQueue> getMessageQueueList() {
        return messageQueueList;
    }

    public void setMessageQueueList(List<MessageQueue> messageQueueList) {
        this.messageQueueList = messageQueueList;
    }

    public ThreadLocalIndex getSendWhichQueue() {
        return sendWhichQueue;
    }

    public void setSendWhichQueue(ThreadLocalIndex sendWhichQueue) {
        this.sendWhichQueue = sendWhichQueue;
    }

    public boolean isHaveTopicRouterInfo() {
        return haveTopicRouterInfo;
    }

    public void setHaveTopicRouterInfo(boolean haveTopicRouterInfo) {
        this.haveTopicRouterInfo = haveTopicRouterInfo;
    }


    /**
     * 该消息在某一次消息的发送后面的重试逻辑中，能规避故障的Broker,
     * 但是如果Broker宕机，由于messageQueueList是按照Broker排序的，
     * 如果上一次选择是宕机的Broker的第一个队列，那么随后选择的可能就是宕机的Broker的第二个队列那么还是会发送失败，
     * 再次引发重试才能规避此Broker，带来不必要的性能损耗。
     */
    public MessageQueue selectOneMessageQueue(final String lastBrokerName) {
        if (lastBrokerName == null) {
            return selectOneMessageQueue();
        } else {
            //通过sendWhichQueue获取自增至
            int index = this.sendWhichQueue.getAndIncrement();
            for (int i = 0; i < this.messageQueueList.size(); i++) {
                //与当前路由表中消息队列个数取模，作为数组下标
                int pos = Math.abs(index++) % this.messageQueueList.size();
                //下标越界则取0
                if (pos < 0)
                    pos = 0;
                MessageQueue mq = this.messageQueueList.get(pos);
                //为了规避上次选中的MessageQueue所在的Broker。否则还是可能再次失败。
                if (!mq.getBrokerName().equals(lastBrokerName)) {
                    return mq;
                }
            }
            //如果上面没有找到则随机选择一个
            return selectOneMessageQueue();
        }
    }

    public MessageQueue selectOneMessageQueue() {
        int index = this.sendWhichQueue.getAndIncrement();
        int pos = Math.abs(index) % this.messageQueueList.size();
        if (pos < 0)
            pos = 0;
        return this.messageQueueList.get(pos);
    }

    public int getQueueIdByBroker(final String brokerName) {
        for (int i = 0; i < topicRouteData.getQueueDatas().size(); i++) {
            final QueueData queueData = this.topicRouteData.getQueueDatas().get(i);
            if (queueData.getBrokerName().equals(brokerName)) {
                return queueData.getWriteQueueNums();
            }
        }

        return -1;
    }

    @Override
    public String toString() {
        return "TopicPublishInfo [orderTopic=" + orderTopic + ", messageQueueList=" + messageQueueList
            + ", sendWhichQueue=" + sendWhichQueue + ", haveTopicRouterInfo=" + haveTopicRouterInfo + "]";
    }

    public TopicRouteData getTopicRouteData() {
        return topicRouteData;
    }

    public void setTopicRouteData(final TopicRouteData topicRouteData) {
        this.topicRouteData = topicRouteData;
    }
}

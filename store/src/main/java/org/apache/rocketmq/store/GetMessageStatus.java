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
package org.apache.rocketmq.store;

public enum GetMessageStatus {

    /**
     * 找到消息
     */
    FOUND,

    /**
     * 消息表达式等过滤之后 没有匹配的消息
     */
    NO_MATCHED_MESSAGE,

    /**
     * 消息存放在下个commotlog文件中
     */
    MESSAGE_WAS_REMOVING,

    /**
     * ConsumeQueue文件中未找到指定偏移量的消息
     * 将会将当前offset定位到下一个consumeQueue文件的offset，下次拉取时使用此offset拉取
     */
    OFFSET_FOUND_NULL,

    /**
     * 表示偏移量越界
     * 待拉取偏移量大于队列的最大偏移量
     * offset越界
     */
    OFFSET_OVERFLOW_BADLY,

    /**
     * 待拉取偏移量等于队列的最大偏移量
     * offset刚好越界
     * 如果之后有新消息到达，那么会创建一个新的ConsumeQueue文件，那么上一个文件的最大偏移量就是下一次文件的起始偏移量，
     * 所以按照该offset下一次拉取消息时能成功。
     * 下次拉取偏移量仍然为offset
     */
    OFFSET_OVERFLOW_ONE,

    /**
     * 待拉取偏移量小于队列的起始偏移量
     * offset未在消息队列中
     */
    OFFSET_TOO_SMALL,

    /**
     * 未找到topic-queueId下的consumeQueue队列
     */
    NO_MATCHED_LOGIC_QUEUE,

    /**
     * topic-queueid对应的消费队列中无消息
     */
    NO_MESSAGE_IN_QUEUE,
}

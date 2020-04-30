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

package org.apache.rocketmq.common.protocol;

public class RequestCode {

    /**
     * Producer向Broker 发送消息
     */
    public static final int SEND_MESSAGE = 10;

    /**
     * 消费者 从Broker服务器中拉取消息。
     */
    public static final int PULL_MESSAGE = 11;

    // Broker 查询消息
    public static final int QUERY_MESSAGE = 12;
    // Broker 查询Broker Offset
    public static final int QUERY_BROKER_OFFSET = 13;
    /**
     * Broker 查询Consumer Offset
     * Consumer查询ConsumerGroup下的topic下的指定消费队列的消费偏移量(ConsumeQueue中的偏移量)
     */
    public static final int QUERY_CONSUMER_OFFSET = 14;
    /**
     * 消费者向Broker更新ConsumeQueue的偏移量
     */
    public static final int UPDATE_CONSUMER_OFFSET = 15;
    // Broker 更新或者增加一个Topic
    public static final int UPDATE_AND_CREATE_TOPIC = 17;
    // Broker 获取所有Topic的配置（Slave和Namesrv都会向Master请求此配置）
    public static final int GET_ALL_TOPIC_CONFIG = 21;
    // Broker 获取所有Topic配置（Slave和Namesrv都会向Master请求此配置）
    public static final int GET_TOPIC_CONFIG_LIST = 22;

    // Broker 获取所有Topic名称列表
    public static final int GET_TOPIC_NAME_LIST = 23;

    // Broker 更新Broker上的配置
    public static final int UPDATE_BROKER_CONFIG = 25;

    // Broker 获取Broker上的配置
    public static final int GET_BROKER_CONFIG = 26;

    // Broker 触发Broker删除文件
    public static final int TRIGGER_DELETE_FILES = 27;

    // Broker 获取Broker运行时信息
    public static final int GET_BROKER_RUNTIME_INFO = 28;
    // Broker 根据时间查询队列的Offset

    /**
     * Consumer根据消息存储时间戳查询ConsumerGroup下的topic下的指定消费队列的消费偏移量(ConsumeQueue中的偏移量)
     */
    public static final int SEARCH_OFFSET_BY_TIMESTAMP = 29;
    /**
     *
     * Consumer查询Broker消费队列最大的偏移量
     */
    public static final int GET_MAX_OFFSET = 30;
    // Broker 查询队列最小Offset
    public static final int GET_MIN_OFFSET = 31;

    // Broker 查询队列最早消息对应时间
    public static final int GET_EARLIEST_MSG_STORETIME = 32;


    /**
     * 根据消息唯一id查询消息
     * 消息id，此消息id记录了消息所在broker的ip+port，以及消息的物理偏移量。
     * @see MessageDecoder#createMessageId(java.nio.ByteBuffer, java.nio.ByteBuffer, long)
     * 因此可以通过此msgId解析出消息的物理偏移量从而查找消息
     */
    public static final int VIEW_MESSAGE_BY_ID = 33;

    /**
     * Client（消费者与生产者）向Broker发送心跳，并注册自身
     */
    public static final int HEART_BEAT = 34;

    // Broker Client注销
    public static final int UNREGISTER_CLIENT = 35;

    /**
     * Consumer将消费失败的的消息发回服务器
     *
     * 如果消息监听器返回 RECONSUME_LATER ，则需要将这些消息发送给Broker延迟消息。
     */
    public static final int CONSUMER_SEND_MSG_BACK = 36;

    /**
     * 生产者发送结束事务命令给到broker
     * 提交事务或者回滚事务或者 不作为的命令
     */
    public static final int END_TRANSACTION = 37;
    /**
     * 客户端通过消费者名获取同一个消费者名下的所有客户端id
     */
    public static final int GET_CONSUMER_LIST_BY_GROUP = 38;

    /**
     * 事务消息时Broker向Producer回查事务状态
     */
    public static final int CHECK_TRANSACTION_STATE = 39;

    /**
     * Broker Broker通知Consumer列表变化，Consumer立马进行消息消费队列的重新负载均衡
     */
    public static final int NOTIFY_CONSUMER_IDS_CHANGED = 40;

    /**
     * 顺序消费时，消费者向master Broker锁定消息消费队列
     */
    public static final int LOCK_BATCH_MQ = 41;

    /**
     * 顺序消费时，消费者向master Broker解锁消息消费队列的请求参数
     */
    public static final int UNLOCK_BATCH_MQ = 42;
    // Broker 获取所有Consumer Offset
    public static final int GET_ALL_CONSUMER_OFFSET = 43;

    // Broker 获取所有定时进度
    public static final int GET_ALL_DELAY_OFFSET = 45;

    public static final int CHECK_CLIENT_CONFIG = 46;

    // Namesrv 向Namesrv追加KV配置
    public static final int PUT_KV_CONFIG = 100;

    // Namesrv 从Namesrv获取KV配置
    public static final int GET_KV_CONFIG = 101;

    // Namesrv 从Namesrv获取KV配置
    public static final int DELETE_KV_CONFIG = 102;

    /**
     * broker每30s发送一个此编码的心跳包
     * broker注册命令编码
     */
    public static final int REGISTER_BROKER = 103;

    /**
     * broker正常关闭，移除Broker心跳包
     */
    public static final int UNREGISTER_BROKER = 104;

    /**
     * 当topic变化后，nameserver不主动推送给客户端，而是由客户端（Producer）定时拉取topic最新路由信息。
     * 根据topic拉取路由信息的命令编码:GET_ROUTEINTO_BY_TOPIC
     */
    public static final int GET_ROUTEINTO_BY_TOPIC = 105;

    // Namesrv 获取注册到Name Server的所有Broker集群信息
    public static final int GET_BROKER_CLUSTER_INFO = 106;
    /**
     * 更新或者创建消费组配置信息。不管之前Broker有没有缓存，都以当前为准
     */
    public static final int UPDATE_AND_CREATE_SUBSCRIPTIONGROUP = 200;
    public static final int GET_ALL_SUBSCRIPTIONGROUP_CONFIG = 201;
    public static final int GET_TOPIC_STATS_INFO = 202;
    /**
     * 根据consumeGroup获取消费组信息
     */
    public static final int GET_CONSUMER_CONNECTION_LIST = 203;
    /**
     * 获取生产者组 信息
     * 返回 生产者客户端id 生产者地址等
     */
    public static final int GET_PRODUCER_CONNECTION_LIST = 204;
    public static final int WIPE_WRITE_PERM_OF_BROKER = 205;

    // 从Name Server获取完整Topic列表
    public static final int GET_ALL_TOPIC_LIST_FROM_NAMESERVER = 206;
    // 从Broker删除订阅组
    public static final int DELETE_SUBSCRIPTIONGROUP = 207;
    /**
     * 从Broker获取消费组的消费进度
     */
    public static final int GET_CONSUME_STATS = 208;
    // Suspend Consumer消费过程
    public static final int SUSPEND_CONSUMER = 209;
    // Resume Consumer消费过程
    public static final int RESUME_CONSUMER = 210;
    // 重置Consumer Offset
    public static final int RESET_CONSUMER_OFFSET_IN_CONSUMER = 211;
    // 重置Consumer Offset
    public static final int RESET_CONSUMER_OFFSET_IN_BROKER = 212;
    // 调整Consumer线程池数量
    public static final int ADJUST_CONSUMER_THREAD_POOL = 213;
    // 查询消息被哪些消费组消费
    public static final int WHO_CONSUME_THE_MESSAGE = 214;
    // 从Broker删除Topic配置
    public static final int DELETE_TOPIC_IN_BROKER = 215;
    // 从Namesrv删除Topic配置
    public static final int DELETE_TOPIC_IN_NAMESRV = 216;
    // Namesrv 通过 project 获取所有的 server ip 信息
    public static final int GET_KV_CONFIG_BY_VALUE = 217;
    // Namesrv 删除指定 project group 下的所有 server ip 信息
    public static final int DELETE_KV_CONFIG_BY_VALUE = 218;
    // 通过NameSpace获取所有的KV List
    public static final int GET_KVLIST_BY_NAMESPACE = 219;
    // offset 重置
    public static final int RESET_CONSUMER_CLIENT_OFFSET = 220;
    // 客户端订阅消息
    public static final int GET_CONSUMER_STATUS_FROM_CLIENT = 221;
    /**
     * 通知 broker 重置消息消费组的消费进度，根据时间
     */
    public static final int INVOKE_BROKER_TO_RESET_OFFSET = 222;
    // 通知 broker 调用客户端订阅消息处理
    public static final int INVOKE_BROKER_TO_GET_CONSUMER_STATUS = 223;

    /**
     * 根据topic去broker查询对应消费组
     */
    public static final int QUERY_TOPIC_CONSUME_BY_WHO = 300;

    // 获取指定集群下的所有 topic
    public static final int GET_TOPICS_BY_CLUSTER = 224;

    // 向Broker注册Filter Server
    public static final int REGISTER_FILTER_SERVER = 301;
    // 向Filter Server注册Class
    public static final int REGISTER_MESSAGE_FILTER_CLASS = 302;

    // 根据 topic 和 group 获取消息的时间跨度
    public static final int QUERY_CONSUME_TIME_SPAN = 303;

    // 获取所有系统内置 Topic 列表
    public static final int GET_SYSTEM_TOPIC_LIST_FROM_NS = 304;
    public static final int GET_SYSTEM_TOPIC_LIST_FROM_BROKER = 305;

    // 清理失效队列
    public static final int CLEAN_EXPIRED_CONSUMEQUEUE = 306;

    // 通过Broker查询Consumer内存数据
    public static final int GET_CONSUMER_RUNNING_INFO = 307;

    // 查找被修正 offset (转发组件）
    public static final int QUERY_CORRECTION_OFFSET = 308;
    // 通过Broker直接向某个Consumer发送一条消息，并立刻消费，返回结果给broker，再返回给调用方
    public static final int CONSUME_MESSAGE_DIRECTLY = 309;

    /**
     * Broker 发送消息，优化网络数据包
     * 使用
     * @see org.apache.rocketmq.common.protocol.header.SendMessageRequestHeaderV2
     * 代替
     * @see org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader
     * 详见
     * @see org.apache.rocketmq.client.impl.MQClientAPIImpl#sendMessage(java.lang.String, java.lang.String, org.apache.rocketmq.common.message.Message, org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader, long, org.apache.rocketmq.client.impl.CommunicationMode, org.apache.rocketmq.client.producer.SendCallback, org.apache.rocketmq.client.impl.producer.TopicPublishInfo, org.apache.rocketmq.client.impl.factory.MQClientInstance, int, org.apache.rocketmq.client.hook.SendMessageContext, org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl)
     */
    public static final int SEND_MESSAGE_V2 = 310;

    // 单元化相关 topic
    public static final int GET_UNIT_TOPIC_LIST = 311;

    // 获取含有单元化订阅组的 Topic 列表
    public static final int GET_HAS_UNIT_SUB_TOPIC_LIST = 312;
    // 获取含有单元化订阅组的非单元化 Topic 列表
    public static final int GET_HAS_UNIT_SUB_UNUNIT_TOPIC_LIST = 313;
    // 克隆某一个组的消费进度到新的组
    public static final int CLONE_GROUP_OFFSET = 314;
    // 查看Broker上的各种统计信息
    public static final int VIEW_BROKER_STATS_DATA = 315;

    public static final int CLEAN_UNUSED_TOPIC = 316;

    public static final int GET_BROKER_CONSUME_STATS = 317;

    /**
     * update the config of name server
     */
    public static final int UPDATE_NAMESRV_CONFIG = 318;

    /**
     * get config from name server
     */
    public static final int GET_NAMESRV_CONFIG = 319;

    /**
     * producer批量发送消息
     */
    public static final int SEND_BATCH_MESSAGE = 320;

    public static final int QUERY_CONSUME_QUEUE = 321;

    public static final int QUERY_DATA_VERSION = 322;
}

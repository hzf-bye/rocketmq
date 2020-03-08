#####1.消息生产者每隔30s更新topic路由信息，在哪里
> 更新至org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl.topicPublishInfoTable中
#####2.Broker处理Producer发送的消息请求
> 前置会检测消息是否合法，其中有一个判断是如果Broker没有写权限且是topic对应的是顺序消息则直接返回错误信息。目前不理解。
> org.apache.rocketmq.broker.processor.AbstractSendMessageProcessor.msgCheck
#####3.MixAll.RETRY_GROUP_TOPIC_PREFIX
> Procuder发送消息时topic以%RETRY%开头作用是啥，以及与之关联的org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader.reconsumeTimes作用
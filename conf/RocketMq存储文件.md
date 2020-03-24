*RocketMQ默认存储路径为${ROCKETMQ_HOME}/store。*

- commitlog：消息存储目录
- config：
   - consumerFilter.json：主题消息过滤信息。
   - consumerOffset.json：集群消费模式消息消费进度。
   - delayOffset.json：延时消息队列拉取进度。
   - subscriptGroup.json：消息消费组配置信息。
   - topics.json：topic配置属性。
- consumerqueue：消息消费队列存储目录。
- index：消息索引文件存储目录。
> 消息消费队列文件、消息索引文件都是基于CommitLog文件构造，当消息生产者提交的消息存储在CommitLog文件中，ConsumeQueue、IndexFile需要及时更新，
否则消息无法及时被消费，根据消息属性来查找消息也会出现较大延迟。RocketMQ通过一个线程ReputMessageService来准时转发CommitLog文件更新事件，
相应的任务处理器根据转发的消息及时更新ConsumerQueue/IndexFile文件。
- abort：如果存在abort文件说明Broker非正常关闭，该文件默认启动时创建，正常退出之前删除。
- checkpoint：文件监测点，存储commitlog文件最后一次刷盘时间戳、consumerqueue最后一次刷盘时间戳、index索引文件最后一次刷盘时间戳。
文件固定长度为4k，只用该文件前面24个字节。分别是8个字节的physicMsgTimestamp：commitlog文件刷盘时间点，
8个字节的logicsMsgTimestamp：消息消费队列文件刷盘时间点，8个字节的indexMsgTimestamp：索引文件文件刷盘时间点。
  




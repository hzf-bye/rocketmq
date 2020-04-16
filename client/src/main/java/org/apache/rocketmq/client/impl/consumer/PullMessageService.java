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
package org.apache.rocketmq.client.impl.consumer;

import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.utils.ThreadUtils;

/**
 * 1. PullRequest对象在什么时候加入到pullRequestQueue中，以便于{@link PullMessageService#run()}
 * 中能拿到PullRequest对象，进行消息的拉取？
 * @see org.apache.rocketmq.client.impl.consumer.RebalanceService#RebalanceService(org.apache.rocketmq.client.impl.factory.MQClientInstance)
 * 线程每隔20s对消费者订阅信息的主体进行一次队列重新分配，每一次分配会对topic下的所有队列、从Broker中实时查询当前该主题该消费组内消费者列表，
 * 对新分配的消息队列会创建对应的PullRequest对象，在一个JVM进程中，同一消费组同一个队列只会存在一个PullRequest对象。
 *
 * 2. 集群内多个消费者是如何负载topic下的多个消费者队列，并且如果有新的消费者加入时，消息队列如何重新分布？
 *
 * 由于每次进行队列重新负载时会从Broker实时查询出当前消费组内所有消费者{@link org.apache.rocketmq.broker.client.ConsumerManager#consumerTable}，并且对消费队列、消费者列表进行排序，
 * 并且有新的消费者加入时会定时向Broker发送心跳信息，并且注册自己{@link MQClientInstance#sendHeartbeatToAllBrokerWithLock()}
 * 这样新加入的消费者就会在队列重新分布时分配到消费队列从而进行消息消费。
 */
public class PullMessageService extends ServiceThread {
    private final InternalLogger log = ClientLogger.getLog();
    /**
     * PullMessageService提供延迟添加与立即添加2中方式将PullRequest放入到队列中。
     * @see PullMessageService#executePullRequestLater(org.apache.rocketmq.client.impl.consumer.PullRequest, long)
     * @see PullMessageService#executePullRequestImmediately(org.apache.rocketmq.client.impl.consumer.PullRequest)
     */
    private final LinkedBlockingQueue<PullRequest> pullRequestQueue = new LinkedBlockingQueue<PullRequest>();
    private final MQClientInstance mQClientFactory;
    private final ScheduledExecutorService scheduledExecutorService = Executors
        .newSingleThreadScheduledExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "PullMessageServiceScheduledThread");
            }
        });

    public PullMessageService(MQClientInstance mQClientFactory) {
        this.mQClientFactory = mQClientFactory;
    }

    public void executePullRequestLater(final PullRequest pullRequest, final long timeDelay) {
        if (!isStopped()) {
            this.scheduledExecutorService.schedule(new Runnable() {
                @Override
                public void run() {
                    PullMessageService.this.executePullRequestImmediately(pullRequest);
                }
            }, timeDelay, TimeUnit.MILLISECONDS);
        } else {
            log.warn("PullMessageServiceScheduledThread has shutdown");
        }
    }

    /**
     * 通过调用链可以发现主要有两个地方会调用，
     * 1. 是在RockerMq根据pullRequest拉取任务执行完一次消息拉取后，又将PullRequest放入到pullRequestQueue
     *      {@link DefaultMQPushConsumerImpl#pullMessage(org.apache.rocketmq.client.impl.consumer.PullRequest)}
     * 2. 在RebalancePushImp中创建。
     * @param pullRequest
     */
    public void executePullRequestImmediately(final PullRequest pullRequest) {
        try {
            this.pullRequestQueue.put(pullRequest);
        } catch (InterruptedException e) {
            log.error("executePullRequestImmediately pullRequestQueue.put", e);
        }
    }

    public void executeTaskLater(final Runnable r, final long timeDelay) {
        if (!isStopped()) {
            this.scheduledExecutorService.schedule(r, timeDelay, TimeUnit.MILLISECONDS);
        } else {
            log.warn("PullMessageServiceScheduledThread has shutdown");
        }
    }

    public ScheduledExecutorService getScheduledExecutorService() {
        return scheduledExecutorService;
    }

    private void pullMessage(final PullRequest pullRequest) {

        /*
         * 根据消费组名从MQClientInstance中获取消费者内部实现类MQConsumerInner，
         * 这里将consumer直接强转为DefaultMQPushConsumerImpl，也就是PullMessageService值为PUSH模式服务
         * 那拉(pull)模式如何拉取消息呢？其实细想也不难理解，PULL模式，RocketMq只需要提供拉取消息API即可，
         * 具体由应用程序显示调用拉取API。
         */
        final MQConsumerInner consumer = this.mQClientFactory.selectConsumer(pullRequest.getConsumerGroup());
        if (consumer != null) {
            DefaultMQPushConsumerImpl impl = (DefaultMQPushConsumerImpl) consumer;
            impl.pullMessage(pullRequest);
        } else {
            log.warn("No matched consumer for the PullRequest {}, drop it", pullRequest);
        }
    }

    /**
     *
     */
    @Override
    public void run() {
        log.info(this.getServiceName() + " service started");

        /**
         * 这是一种通用的设计技巧，stopped声明为volatile，每执行一次业务逻辑检测一下其运行状态，可以通过其他线程将stopped设置为ture，从而停止该线程运行。
         */
        while (!this.isStopped()) {
            try {
                /**
                 * 从pullRequestQueue获取一个PullRequest消息拉取任务，如果pullRequestQueue为空，线程将阻塞，知道有任务被放入队列中。
                 * @see PullMessageService#executePullRequestLater(org.apache.rocketmq.client.impl.consumer.PullRequest, long)  中将任务放入队列中。
                 */
                PullRequest pullRequest = this.pullRequestQueue.take();
                this.pullMessage(pullRequest);
            } catch (InterruptedException ignored) {
            } catch (Exception e) {
                log.error("Pull Message Service Run Method exception", e);
            }
        }

        log.info(this.getServiceName() + " service end");
    }

    @Override
    public void shutdown(boolean interrupt) {
        super.shutdown(interrupt);
        ThreadUtils.shutdownGracefully(this.scheduledExecutorService, 1000, TimeUnit.MILLISECONDS);
    }

    @Override
    public String getServiceName() {
        return PullMessageService.class.getSimpleName();
    }

}

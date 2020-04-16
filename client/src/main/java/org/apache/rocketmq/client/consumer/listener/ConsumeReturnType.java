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
import org.apache.rocketmq.client.impl.consumer.ConsumeMessageOrderlyService;

/**
 * @see ConsumeMessageConcurrentlyService.ConsumeRequest#run()
 * @see ConsumeMessageOrderlyService.ConsumeRequest#run()
 */
public enum ConsumeReturnType {
    /**
     * consume return success
     * 消息消费成功
     */
    SUCCESS,
    /**
     * consume timeout ,even if success
     * 消息消费超时，默认超时时间15分钟
     */
    TIME_OUT,
    /**
     * consume throw exception
     * 消息消费过程中出现异常
     */
    EXCEPTION,
    /**
     * consume return null
     * 消息消费后返回 null
     */
    RETURNNULL,
    /**
     * consume return failed
     * 消息消费后返回
     * @see ConsumeConcurrentlyStatus#RECONSUME_LATER
     */
    FAILED
}

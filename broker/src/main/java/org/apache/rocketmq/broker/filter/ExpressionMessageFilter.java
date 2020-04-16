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

package org.apache.rocketmq.broker.filter;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.filter.util.BitsArray;
import org.apache.rocketmq.filter.util.BloomFilter;
import org.apache.rocketmq.store.ConsumeQueueExt;
import org.apache.rocketmq.store.MessageFilter;

import java.nio.ByteBuffer;
import java.util.Map;

public class ExpressionMessageFilter implements MessageFilter {

    protected static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.FILTER_LOGGER_NAME);

    /**
     * topic订阅信息
     */
    protected final SubscriptionData subscriptionData;
    protected final ConsumerFilterData consumerFilterData;
    protected final ConsumerFilterManager consumerFilterManager;
    protected final boolean bloomDataValid;

    public ExpressionMessageFilter(SubscriptionData subscriptionData, ConsumerFilterData consumerFilterData,
        ConsumerFilterManager consumerFilterManager) {
        this.subscriptionData = subscriptionData;
        this.consumerFilterData = consumerFilterData;
        this.consumerFilterManager = consumerFilterManager;
        if (consumerFilterData == null) {
            bloomDataValid = false;
            return;
        }
        BloomFilter bloomFilter = this.consumerFilterManager.getBloomFilter();
        if (bloomFilter != null && bloomFilter.isValid(consumerFilterData.getBloomFilterData())) {
            bloomDataValid = true;
        } else {
            bloomDataValid = false;
        }
    }

    /**
     * 根据consumeQueue判断消息是否匹配
     * @param tagsCode tagsCode 消息tag的hashCode
     * @param cqExtUnit extend unit of consume queue ConsumeQueue条目扩展属性
     */
    @Override
    public boolean isMatchedByConsumeQueue(Long tagsCode, ConsumeQueueExt.CqExtUnit cqExtUnit) {

        //如果订阅信息为空，返回true，不过滤
        if (null == subscriptionData) {
            return true;
        }

        //如果是类过滤模式，返回true
        if (subscriptionData.isClassFilterMode()) {
            return true;
        }

        // by tags code.
        //以下只是通过tag的hashcode过滤，所以基于tag模式过滤还需要在消息消费端对消息的tag进行精确的过滤。
        if (ExpressionType.isTagType(subscriptionData.getExpressionType())) {

            //TAG过滤模式
            //并且消息的tagsCode为空，说明发发送消息时没有设置TAG，返回true。
            if (tagsCode == null) {
                return true;
            }

            // 消息过滤表达式是*，不需要过滤，返回true
            if (subscriptionData.getSubString().equals(SubscriptionData.SUB_ALL)) {
                return true;
            }

            //如果订阅的tag hashcode集合中包含了当前消息的tagsCode，返回true
            return subscriptionData.getCodeSet().contains(tagsCode.intValue());
        } else {
            // no expression or no bloom
            //布隆过滤器过滤
            if (consumerFilterData == null || consumerFilterData.getExpression() == null
                || consumerFilterData.getCompiledExpression() == null || consumerFilterData.getBloomFilterData() == null) {
                return true;
            }

            // message is before consumer
            if (cqExtUnit == null || !consumerFilterData.isMsgInLive(cqExtUnit.getMsgStoreTime())) {
                log.debug("Pull matched because not in live: {}, {}", consumerFilterData, cqExtUnit);
                return true;
            }

            byte[] filterBitMap = cqExtUnit.getFilterBitMap();
            BloomFilter bloomFilter = this.consumerFilterManager.getBloomFilter();
            if (filterBitMap == null || !this.bloomDataValid
                || filterBitMap.length * Byte.SIZE != consumerFilterData.getBloomFilterData().getBitNum()) {
                return true;
            }

            BitsArray bitsArray = null;
            try {
                bitsArray = BitsArray.create(filterBitMap);
                boolean ret = bloomFilter.isHit(consumerFilterData.getBloomFilterData(), bitsArray);
                log.debug("Pull {} by bit map:{}, {}, {}", ret, consumerFilterData, bitsArray, cqExtUnit);
                return ret;
            } catch (Throwable e) {
                log.error("bloom filter error, sub=" + subscriptionData
                    + ", filter=" + consumerFilterData + ", bitMap=" + bitsArray, e);
            }
        }

        return true;
    }

    /**
     * 根据存储在commitlog文件中的内容判断消息是否匹配
     * 该方法主要是为了表达式模式SQL92服务的
     * @param msgBuffer message buffer in commit log, may be null if not invoked in store. 消息内容，如果为空，该方法fanhuitrue
     * @param properties message properties, should decode from buffer if null by yourself. 消息属性，主要用于SQL92过滤模式
     */
    @Override
    public boolean isMatchedByCommitLog(ByteBuffer msgBuffer, Map<String, String> properties) {

        //如果订阅信息为空，返回true，不过滤
        if (subscriptionData == null) {
            return true;
        }

        //如果是类过滤模式，返回true
        if (subscriptionData.isClassFilterMode()) {
            return true;
        }

        //如果是TAG模式，返回true，因为在isMatchedByConsumeQueue中已经通过TAG hashCode过滤
        if (ExpressionType.isTagType(subscriptionData.getExpressionType())) {
            return true;
        }

        ConsumerFilterData realFilterData = this.consumerFilterData;
        Map<String, String> tempProperties = properties;

        // no expression
        if (realFilterData == null || realFilterData.getExpression() == null
            || realFilterData.getCompiledExpression() == null) {
            return true;
        }

        if (tempProperties == null && msgBuffer != null) {
            tempProperties = MessageDecoder.decodeProperties(msgBuffer);
        }

        Object ret = null;
        try {
            MessageEvaluationContext context = new MessageEvaluationContext(tempProperties);

            ret = realFilterData.getCompiledExpression().evaluate(context);
        } catch (Throwable e) {
            log.error("Message Filter error, " + realFilterData + ", " + tempProperties, e);
        }

        log.debug("Pull eval result: {}, {}, {}", ret, realFilterData, tempProperties);

        if (ret == null || !(ret instanceof Boolean)) {
            return false;
        }

        return (Boolean) ret;
    }

}

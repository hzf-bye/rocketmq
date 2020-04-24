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
package org.apache.rocketmq.common.sysflag;

import org.apache.rocketmq.common.MixAll;

public class MessageSysFlag {
    /**
     * 消息体压缩标志
     * 0b0001
     */
    public final static int COMPRESSED_FLAG = 0x1;
    /**
     * 0b0010
     */
    public final static int MULTI_TAGS_FLAG = 0x1 << 1;
    /**
     * 0b0000
     */
    public final static int TRANSACTION_NOT_TYPE = 0;
    /**
     * 事务PREPARED消息标志
     * 此标记的消息表示消息预提交
     * @see MixAll#RMQ_SYS_TRANS_HALF_TOPIC
     * 会提交至此topic下的消息队列，默认只有一个消息队列
     * 0b0100
     */
    public final static int TRANSACTION_PREPARED_TYPE = 0x1 << 2;
    /**
     * 事务提交
     * 0b1000
     */
    public final static int TRANSACTION_COMMIT_TYPE = 0x2 << 2;
    /**
     * 事务回滚
     * 0b1100
     */
    public final static int TRANSACTION_ROLLBACK_TYPE = 0x3 << 2;

    public static int getTransactionValue(final int flag) {
        return flag & TRANSACTION_ROLLBACK_TYPE;
    }

    /**
     * 清除事务标志位
     * ~TRANSACTION_ROLLBACK_TYPE = 0b0011
     * flag & 0b0011 就是高二位也就是清除事务标志位，
     *
     * 再 | type 得到具体的值
     */
    public static int resetTransactionValue(final int flag, final int type) {
        return (flag & (~TRANSACTION_ROLLBACK_TYPE)) | type;
    }

    public static int clearCompressedFlag(final int flag) {
        return flag & (~COMPRESSED_FLAG);
    }

    public static void main(String[] args) {

        System.out.println(~TRANSACTION_ROLLBACK_TYPE);
        System.out.println(TRANSACTION_PREPARED_TYPE  & (~TRANSACTION_ROLLBACK_TYPE));
        System.out.println(resetTransactionValue(TRANSACTION_PREPARED_TYPE, TRANSACTION_NOT_TYPE));
    }
}

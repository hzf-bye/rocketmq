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

package org.apache.rocketmq.client.common;

import java.util.Objects;
import java.util.Random;

public class ThreadLocalIndex {
    private final ThreadLocal<Integer> threadLocalIndex = new ThreadLocal<Integer>();
    private final Random random = new Random();

    public int getAndIncrement() {
        Integer index = this.threadLocalIndex.get();
        if (null == index) {
            //如果此时index为-2147483648，int的最小值，那么取绝对值还是-2147483648那么就小于0，就重置为0
            //下同
            index = Math.abs(random.nextInt());
            if (index < 0)
                index = 0;
            this.threadLocalIndex.set(index);
        }

        index = Math.abs(index + 1);
        if (index < 0)
            index = 0;

        this.threadLocalIndex.set(index);
        return index;
    }

    @Override
    public String toString() {
        return "ThreadLocalIndex{" +
            "threadLocalIndex=" + threadLocalIndex.get() +
            '}';
    }

}

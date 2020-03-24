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

import java.util.concurrent.atomic.AtomicLong;

public abstract class ReferenceResource {

    /**
     * 引用计数
     */
    protected final AtomicLong refCount = new AtomicLong(1);

    /**
     * 标识是否可用
     * 调用shutdown方法后未false
     */
    protected volatile boolean available = true;

    /**
     * 标识是否是否资源
     * true-{@link ReferenceResource#release()}方法成功将{@link MappedFile#mappedByteBuffer}资源释放。
     */
    protected volatile boolean cleanupOver = false;

    /**
     * 初次关闭的时间戳
     */
    private volatile long firstShutdownTimestamp = 0;

    /**
     * 判断是否有引用，如果有引用那么引用次数再+1，返回true
     * 否则返回false
     * 当每调用一次hold方法返回true后表示当前文件的引用此时+1，
     * 那么在处理完后都将调用release()方法引用次数-1
     */
    public synchronized boolean hold() {
        if (this.isAvailable()) {
            //判断当前引用计数并且+1
            if (this.refCount.getAndIncrement() > 0) {
                //引用>0，返回true
                return true;
            } else {
                //引用<=0，并且if中已经+1了，这里再-1
                this.refCount.getAndDecrement();
            }
        }

        return false;
    }

    public boolean isAvailable() {
        return this.available;
    }

    public void shutdown(final long intervalForcibly) {
        //初次调用时available为true
        if (this.available) {
            //设置为false
            this.available = false;
            //设置初次关闭的时间戳
            this.firstShutdownTimestamp = System.currentTimeMillis();
            //释放资源
            this.release();
        } else if (this.getRefCount() > 0) {
            //当available为false，并且此时还有引用计数，
            //并且第一次关闭时间戳到现在的时间已经大于等于intervalForcibly了
            //那么设置引用计数为负数，代表无引用，并且释放资源
            if ((System.currentTimeMillis() - this.firstShutdownTimestamp) >= intervalForcibly) {
                this.refCount.set(-1000 - this.getRefCount());
                this.release();
            }
        }
    }

    public void release() {
        //引用计数-1
        long value = this.refCount.decrementAndGet();
        //当引用计数 <=0 时才会释放资源
        if (value > 0)
            return;

        synchronized (this) {

            this.cleanupOver = this.cleanup(value);
        }
    }

    public long getRefCount() {
        return this.refCount.get();
    }

    public abstract boolean cleanup(final long currentRef);

    /**
     * 判断资源是否释放，
     * 引用计数<=0且cleanupOver被标记为true
     */
    public boolean isCleanupOver() {
        return this.refCount.get() <= 0 && this.cleanupOver;
    }
}

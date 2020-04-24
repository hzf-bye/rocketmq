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
package org.apache.rocketmq.common.message;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.common.UtilAll;

public class MessageClientIDSetter {
    private static final String TOPIC_KEY_SPLITTER = "#";
    private static final int LEN;
    /**
     * 就是一个客户端固定一个前缀
     * 客户端ip（4字节） + 客户端进程id（2字节） + 加载MessageClientIDSetter的类加载器的hashcode（4字节）
     * 共十个字节
     */
    private static final String FIX_STRING;
    private static final AtomicInteger COUNTER;
    private static long startTime;
    private static long nextStartTime;

    static {
        LEN = 4 + 2 + 4 + 4 + 2;
        ByteBuffer tempBuffer = ByteBuffer.allocate(10);
        //第3、4、5、6个字节存储进程id，占用两个字节，且是大端模式，所以进程id会存储在第5、6个字节处
        tempBuffer.position(2);
        tempBuffer.putInt(UtilAll.getPid());
        //position设置为0，那么低4位存储ip
        tempBuffer.position(0);
        try {
            tempBuffer.put(UtilAll.getIP());
        } catch (Exception e) {
            tempBuffer.put(createFakeIP());
        }
        //第7、8、9、10字节存储加载 MessageClientIDSetter 的类加载器的 hashcode
        tempBuffer.position(6);
        tempBuffer.putInt(MessageClientIDSetter.class.getClassLoader().hashCode());
        FIX_STRING = UtilAll.bytes2string(tempBuffer.array());
        setStartTime(System.currentTimeMillis());
        COUNTER = new AtomicInteger(0);
    }

    public static void main1(String[] args) {

        ByteBuffer tempBuffer = ByteBuffer.allocate(10);
        tempBuffer.position(2);
        tempBuffer.putInt(80);
        tempBuffer.position(0);
        tempBuffer.put(new byte[]{10,10,10,10});
        tempBuffer.position(0);
        byte[] bytes = new byte[10];
        tempBuffer.get(bytes);
        System.out.println();

    }

    private synchronized static void setStartTime(long millis) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(millis);
        cal.set(Calendar.DAY_OF_MONTH, 1);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        startTime = cal.getTimeInMillis();
        cal.add(Calendar.MONTH, 1);
        nextStartTime = cal.getTimeInMillis();
    }

    public static Date getNearlyTimeFromID(String msgID) {
        ByteBuffer buf = ByteBuffer.allocate(8);
        byte[] bytes = UtilAll.string2bytes(msgID);
        buf.put((byte) 0);
        buf.put((byte) 0);
        buf.put((byte) 0);
        buf.put((byte) 0);
        buf.put(bytes, 10, 4);
        buf.position(0);
        long spanMS = buf.getLong();
        Calendar cal = Calendar.getInstance();
        long now = cal.getTimeInMillis();
        cal.set(Calendar.DAY_OF_MONTH, 1);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        long monStartTime = cal.getTimeInMillis();
        if (monStartTime + spanMS >= now) {
            cal.add(Calendar.MONTH, -1);
            monStartTime = cal.getTimeInMillis();
        }
        cal.setTimeInMillis(monStartTime + spanMS);
        return cal.getTime();
    }

    public static String getIPStrFromID(String msgID) {
        byte[] ipBytes = getIPFromID(msgID);
        return UtilAll.ipToIPv4Str(ipBytes);
    }

    public static byte[] getIPFromID(String msgID) {
        byte[] result = new byte[4];
        byte[] bytes = UtilAll.string2bytes(msgID);
        System.arraycopy(bytes, 0, result, 0, 4);
        return result;
    }

    public static String createUniqID() {
        StringBuilder sb = new StringBuilder(LEN * 2);
        //前十个字节存储前缀
        sb.append(FIX_STRING);
        //后6个字节：当前时间与系统启动时间的差值或者（4字节），以及自增序号（2字节）。
        sb.append(UtilAll.bytes2string(createUniqIDBuffer()));
        return sb.toString();
    }

    public static void main(String[] args) {

        String uniqueMsgId = "0A644A05000D77774571685080CB00FE";
        byte[] bytes = UtilAll.string2bytes(uniqueMsgId);

        System.out.println();
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        //生产者ip
        byte[] ip = new byte[4];
        byteBuffer.get(ip);
        System.out.println("生产者ip：" + Arrays.toString(ip));
        //生产者进程id
        System.out.println("生产者进程id：" + byteBuffer.getShort());
        //生产者加载MessageClientIDSetter的类加载器的hashcode（4字节）
        System.out.println("类加载器的hashcode：" + byteBuffer.getInt());
        //生产者发送时间与系统启动时间的差值
        System.out.println("时间的差值：" + byteBuffer.getInt());
        //自增序号
        System.out.println("自增序号：" + byteBuffer.getShort());

    }


    private static byte[] createUniqIDBuffer() {
        ByteBuffer buffer = ByteBuffer.allocate(4 + 2);
        long current = System.currentTimeMillis();
        //下次启动时间，默认为系统启动时间加一个月
        if (current >= nextStartTime) {
            setStartTime(current);
        }
        buffer.position(0);
        buffer.putInt((int) (System.currentTimeMillis() - startTime));
        buffer.putShort((short) COUNTER.getAndIncrement());
        return buffer.array();
    }

    public static void setUniqID(final Message msg) {
        if (msg.getProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX) == null) {
            msg.putProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, createUniqID());
        }
    }

    public static String getUniqID(final Message msg) {
        return msg.getProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
    }

    public static byte[] createFakeIP() {
        ByteBuffer bb = ByteBuffer.allocate(8);
        bb.putLong(System.currentTimeMillis());
        bb.position(4);
        byte[] fakeIP = new byte[4];
        bb.get(fakeIP);
        return fakeIP;
    }
}
    

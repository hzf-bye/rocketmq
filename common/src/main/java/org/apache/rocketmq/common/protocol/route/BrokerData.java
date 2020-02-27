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

package org.apache.rocketmq.common.protocol.route;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

import com.alibaba.fastjson.JSON;
import org.apache.rocketmq.common.MixAll;

/**
 * 主从的brokerName是相同的，只是brokerId不同
 *
 * 同一个集群中的cluster是相同的，brokerName不同。
 *
 * 比如2主2从集群
 *
 * Master                       Slave
 *
 * cluster:c1                   cluster:c1
 * brokerName:broker-a  --->    brokerName:broker-a
 * brokerId:0                   brokerId:1
 *
 * cluster:c1                   cluster:c1
 * brokerName:broker-b  --->    brokerName:broker-b
 * brokerId:0                   brokerId:1
 *
 */
public class BrokerData implements Comparable<BrokerData> {

    private String cluster;
    private String brokerName;
    /**
     * brokerId为0代表Master，大于0代表Slave。
     */
    private HashMap<Long/* brokerId */, String/* broker address */> brokerAddrs;

    private final Random random = new Random();

    public BrokerData() {

    }

    public BrokerData(String cluster, String brokerName, HashMap<Long, String> brokerAddrs) {
        this.cluster = cluster;
        this.brokerName = brokerName;
        this.brokerAddrs = brokerAddrs;
    }

    /**
     * Selects a (preferably master) broker address from the registered list.
     * If the master's address cannot be found, a slave broker address is selected in a random manner.
     *
     * @return Broker address.
     */
    public String selectBrokerAddr() {
        String addr = this.brokerAddrs.get(MixAll.MASTER_ID);

        if (addr == null) {
            List<String> addrs = new ArrayList<String>(brokerAddrs.values());
            return addrs.get(random.nextInt(addrs.size()));
        }

        return addr;
    }

    public HashMap<Long, String> getBrokerAddrs() {
        return brokerAddrs;
    }

    public void setBrokerAddrs(HashMap<Long, String> brokerAddrs) {
        this.brokerAddrs = brokerAddrs;
    }

    public String getCluster() {
        return cluster;
    }

    public void setCluster(String cluster) {
        this.cluster = cluster;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((brokerAddrs == null) ? 0 : brokerAddrs.hashCode());
        result = prime * result + ((brokerName == null) ? 0 : brokerName.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        BrokerData other = (BrokerData) obj;
        if (brokerAddrs == null) {
            if (other.brokerAddrs != null)
                return false;
        } else if (!brokerAddrs.equals(other.brokerAddrs))
            return false;
        if (brokerName == null) {
            if (other.brokerName != null)
                return false;
        } else if (!brokerName.equals(other.brokerName))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "BrokerData [brokerName=" + brokerName + ", brokerAddrs=" + brokerAddrs + "]";
    }

    @Override
    public int compareTo(BrokerData o) {
        return this.brokerName.compareTo(o.getBrokerName());
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }

    public static void main(String[] args) {

        BrokerData brokerData = new BrokerData();
        brokerData.setBrokerName("broker-a");
        brokerData.setCluster("c1");
        HashMap<Long, String> brokerAddrs = new HashMap<Long, String>();
        brokerAddrs.put(0L, "192.168.0.110:10000");
        brokerAddrs.put(1L, "192.168.0.111:10000");
        brokerData.setBrokerAddrs(brokerAddrs);

        BrokerData brokerData1 = new BrokerData();
        brokerData1.setBrokerName("broker-b");
        brokerData1.setCluster("c1");
        HashMap<Long, String> brokerAddrs1 = new HashMap<Long, String>();
        brokerAddrs1.put(0L, "192.168.0.112:10000");
        brokerAddrs1.put(1L, "192.168.0.113:10000");
        brokerData1.setBrokerAddrs(brokerAddrs1);


        HashMap<String/* brokerName */, BrokerData> brokerAddrTable = new HashMap<String, BrokerData>();

        brokerAddrTable.put("broker-a",brokerData);
        brokerAddrTable.put("broker-b",brokerData1);
        System.out.println(JSON.toJSONString(brokerAddrTable));

    }
}

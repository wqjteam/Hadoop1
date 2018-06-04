package com.wqj.kafka;



import org.apache.kafka.clients.producer.Partitioner;

import java.util.Map;

/**
 * @Auther: wqj
 * @Date: 2018/5/28 17:40
 * @Description:
 */
public class MyPartitoner implements Partitioner {


    public int partition(String s, Object o, byte[] bytes, Object o1, byte[] bytes1, org.apache.kafka.common.Cluster cluster) {
        return 0;
    }

    public void close() {

    }

    public void configure(Map<String, ?> map) {

    }
}

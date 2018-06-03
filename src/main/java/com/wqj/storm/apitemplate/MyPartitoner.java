package com.wqj.storm.apitemplate;

import kafka.producer.Partitioner;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Cluster;

/**
 * @Auther: wqj
 * @Date: 2018/5/28 17:40
 * @Description:
 */
public class MyPartitoner implements Partitioner {


    public int partition(Object o, int i) {
        return 0;
    }
}

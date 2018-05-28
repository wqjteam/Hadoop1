package com.wqj.storm.apitemplate;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.internals.Partitioner;
import org.apache.kafka.common.Cluster;

/**
 * @Auther: wqj
 * @Date: 2018/5/28 17:40
 * @Description:
 */
public class MyPartitoner extends Partitioner {
    @Override
    public int partition(ProducerRecord<byte[], byte[]> record, Cluster cluster) {
        return super.partition(record, cluster);
    }
}

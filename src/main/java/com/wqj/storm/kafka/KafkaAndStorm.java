package com.wqj.storm.kafka;


import backtype.storm.topology.TopologyBuilder;

/**
 * @Auther: wqj
 * @Date: 2018/6/1 16:27
 * @Description:
 */
public class KafkaAndStorm {
    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafkaS",new KafkaSpout(),1);
    }
}

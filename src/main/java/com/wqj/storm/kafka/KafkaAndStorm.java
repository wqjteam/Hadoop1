package com.wqj.storm.kafka;


import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import storm.kafka.*;
import storm.kafka.bolt.KafkaBolt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Auther: wqj
 * @Date: 2018/6/1 16:27
 * @Description:
 */
public class KafkaAndStorm {
    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
        BrokerHosts brokerHosts = new ZkHosts("master:2181,slave1:2181,slave2:2181");

        SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, "test", "", "kafkaspout");

        List<String> zkServers = new ArrayList<String>() ;
        zkServers.add("master");
        zkServers.add("slave1");
        zkServers.add("slave2");
        spoutConfig.zkServers = zkServers;
        spoutConfig.zkPort = 2181;
        spoutConfig.socketTimeoutMs = 60 * 1000 ;
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme()) ;
        //自定义的转换
//        spoutConfig.scheme = new SchemeAsMultiScheme(new MessageScheme());



        TopologyBuilder builder = new TopologyBuilder();

        Config conf = new Config();
        Map<String, String> map = new HashMap<String, String>();

        map.put("metadata.broker.list", "master:9092,slave1:9092,slave2:9092");
        map.put("serializer.class", "kafka.serializer.StringEncoder");
        conf.put("kafka.broker.properties", map);
        conf.put("topic", "test");

        builder.setSpout("spout", new KafkaSpout(spoutConfig));
        builder.setBolt("bolt", new MykafkaBolt1(),1).shuffleGrouping("spout");


        if (args != null && args.length > 0) {
            conf.setNumWorkers(1);
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        } else {
            // 这里是本地模式下运行的启动代码。
            conf.setMaxTaskParallelism(1);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("simple", conf, builder.createTopology());
        }
    }
}

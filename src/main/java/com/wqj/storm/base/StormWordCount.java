package com.wqj.storm.base;


import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class StormWordCount {
    /**
     * 功能描述:
     *
     * @param:
     * @return:
     * @auther: wqj
     * @date: 2018/5/21 14:16
     */
    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {

        //1.准备一个TopologyBuilder
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("mySpout",new MySpout(),1);
        //shuffleGrouping随机分发
        topologyBuilder.setBolt("mybolt1",new MySplitBolt(),10).shuffleGrouping("mySpout");
        //fieldsGrouping按照word分发
        topologyBuilder.setBolt("mybolt2",new MyCountBolt(),2).fieldsGrouping("mybolt1",new Fields("word"));

        //2.创建一个config 用来定义当前的topology,需要的worker数量
        Config config = new Config();
        config.setNumWorkers(2);
        //3.提交任务 --两种模式:本地模式|集群模式
//        StormSubmitter.submitTopology("mywordcount",config,topologyBuilder.createTopology());

        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("mywordcount",config,topologyBuilder.createTopology());


    }
}

package com.wqj.storm.base;



import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

/**
 * @Auther: wqj
 * @Date: 2018/5/21 15:20
 * @Description:
 */
public class MySplitBolt extends BaseRichBolt {
    OutputCollector collector=null;
    //初始化方法
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector=collector;
    }

    //执行的主方法,相当于循环调用,这个tupkle 是从sqout中获取,拿的化就有技巧
    public void execute(Tuple input) {
        String string = input.getString(0);

        //切割数组
        String[] split = string.split(" ");
        for (String word:split) {
            collector.emit(new Values(word,1));
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

        declarer.declare(new Fields("word","number"));
    }
}

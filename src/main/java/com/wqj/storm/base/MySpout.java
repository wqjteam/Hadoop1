package com.wqj.storm.base;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * @Auther: wqj
 * @Date: 2018/5/21 15:18
 * @Description:
 */
public class MySpout extends BaseRichSpout {

    SpoutOutputCollector collector;
    /**
     *
     * 功能描述: 初始化方法
     *
     * @param:
     * @return:
     * @auther: wqj
     * @date: 2018/5/21 15:21
     */
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector=collector;
    }

    /**
     *
     * 功能描述:相当于while(true),相当于不听的循环
     *
     * @param:
     * @return:
     * @auther: wqj
     * @date: 2018/5/21 15:23
     */
    public void nextTuple() {
        //输出到下一次 bolt
        collector.emit(new Values("i am wqj is body"));
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        //作为声明
        declarer.declare(new Fields("aaa"));
    }
}

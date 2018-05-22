package com.wqj.storm.base;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 * @Auther: wqj
 * @Date: 2018/5/21 16:43
 * @Description:
 */
public class MyCountBolt extends BaseRichBolt {
    OutputCollector collector = null;

    Map<String, Integer> map = new HashMap<String, Integer>();

    //初始化
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    //处理主业务逻辑
    public void execute(Tuple input) {
        String word = input.getString(0); //取得就是 word
        Integer number = input.getInteger(1); //取得就是number

        //将其for循环 ,如果存在就给他加一
        if(map.containsKey(word)){
            Integer i=map.get(word);
            //map中存在相同的话 就会替换
            map.put(word,i+number);
        }else{
            map.put(word,number);
        }
        System.out.println("======================================count"+map);
    }

    //描述方法
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

        //没有下一个bolt或者spout  就不需要编写代码
    }
}

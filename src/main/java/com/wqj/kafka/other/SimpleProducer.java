package com.wqj.kafka.other;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * @Auther: wqj
 * @Date: 2018/6/3 20:47
 * @Description:
 */
public class SimpleProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
//        props.put("zk.connect", "master:2181,slave1:2181,slave2:2181");
        // serializer.class为消息的序列化类
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        // 配置metadata.broker.list, 为了高可用, 最好配两个broker实例
        props.put("metadata.broker.list", "master:9092,slave1:9092,slave2:9092");
        // ACK机制, 消息发送需要kafka服务端确认
//        props.put("request.required.acks", "1");

        props.put("num.partitions", "3");
        ProducerConfig config = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<String, String>(config);
        for (int i = 0; i < 10; i++) {
            SimpleDateFormat formatter = new SimpleDateFormat("yyyy年MM月dd日 HH:mm:ss SSS");
            Date curDate = new Date(System.currentTimeMillis());
            String str = formatter.format(curDate);

            String msg = "test" + i + "=" + str;
            String key = i + "";

            /**
             * KeyedMessage<K, V>,K对应Partition Key的类型,V对应消息本身的类型
             * topic: "test", key: "key", message: "message"
             */
            producer.send(new KeyedMessage<String, String>("test",key, msg));
            System.out.println("发送成功");
        }
    }
}

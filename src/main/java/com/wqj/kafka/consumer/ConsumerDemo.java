package com.wqj.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Map;
import java.util.Properties;

/**
 * @Auther: wqj
 * @Date: 2018/6/3 16:19
 * @Description:  此版的client还没有实现
 */
public class ConsumerDemo {
    public static void main(String[] args){
        Properties properties = new Properties();
        properties.put("zookeeper.connect", "master:2181,slave1:2181,slave2:2181");
        properties.put("bootstrap.servers", "master:9092,slave1:9092,slave2:9092");
        properties.put("client.id","ad");
        properties.put("group.id", "group-1");
        properties.put("zookeeper.session.timeout.ms", "400");
        properties.put("zookeeper.sync.time.ms", "200");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("partition.assignment.strategy", "range");
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);

        //加入监听的topic
        kafkaConsumer.subscribe("test");

        while (true) {
//            Map<String, ConsumerRecords<String, String>> poll = kafkaConsumer.poll(100);
            Map<String, ConsumerRecords<String, String>> poll = kafkaConsumer.poll(100);

            final ConsumerRecords<String, String> test = poll.get("test");
//            for (ConsumerRecord<String, String> record : test) {
//                System.out.printf("offset = %d, value = %s", record.offset(), record.value());
//                System.out.println();
//            }
        }

    }
}

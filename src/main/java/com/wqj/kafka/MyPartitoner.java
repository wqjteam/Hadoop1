package com.wqj.kafka;





import kafka.producer.Partitioner;

import java.util.Map;

/**
 * @Auther: wqj
 * @Date: 2018/5/28 17:40
 * @Description:
 */
public class MyPartitoner implements Partitioner {


    @Override
    public int partition(Object o, int i) {
        return 1;
    }
}

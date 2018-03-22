package com.wqj.mapreduces.numbercount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @Description: TODO
 * @author wqj
 * @date 2018年3月20日 下午1:56:07
 *  参数对应 map的参数 
 */
public class ReducesDemo extends Reducer<Text, IntWritable, Text, IntWritable>{

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Context ctx) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		int count = 0; 
		
		for (IntWritable value : values) {
			count  +=value.get();
		}
		ctx.write(key, new IntWritable(count));
	}



	
	
}

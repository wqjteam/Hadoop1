package com.wqj.mapreduces;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @Description: TODO
 * @author wqj
 * @date 2018年3月20日 下午1:29:35 
 * KEYIN 默认情况下是 maprduces 所督导一行文本的偏移量 Long
 * 	在hadoop中用LongWritable
 * VALUEIN 默认情况下mr框架所读到的内容 String 用TEXT
 * KEYOUT 是用户自定义逻辑处理完成之后输出的KEY 在此处是单词 String
 * VALUEOUT 是用户自定义逻辑处理完成之后输出的value,此处是单词次数,Inetger
 */
public class MapDemo extends Mapper<LongWritable, Text, Text, IntWritable>{

	/**
	 * @Description: map阶段的自定义业务逻辑,maptask会对每一行数据调用一次我们自定义map()方法
	 * author wqj
	 * @param key
	 * @param value
	 * @param context
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		//将maptask 传给我们的文本内容 转为String
		String line = value.toString();
		
		//根据空格查分每一个单词
		String[] words = line.split(" ");
		
		for (String word : words) {
			//将单词作为key,  次数作为value
			context.write(new Text(word), new IntWritable());
		}
		
		
	}
	
}

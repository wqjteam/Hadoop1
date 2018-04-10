package com.wqj.mapreduces.phoneflow;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.wqj.mapreduces.numbercount.MapDemo;
import com.wqj.mapreduces.numbercount.ReducesDemo;
import com.wqj.mapreduces.numbercount.WordCountDriver;

public class FlowCount {
	public static void main(String[] args) throws Exception{
		Configuration configuration = new Configuration();
		
		configuration.set("mapreduce.framework.name", "yarn");
		configuration.set("yarn.resourcesmanage.hostname", "master");
//		configuration.set("fs.defaultFS", "hdfs://master:9000");
		
//		configuration.set("","");
		Job job = Job.getInstance(configuration);

		// job.setJar(jar);
		// 指定本程序所在jar的所在路进
		job.setJarByClass(FlowCount.class);

		// 指定本业务的job要是用的map和maprducer类
		job.setMapperClass(FlowMap.class);
		job.setReducerClass(FlowReducer.class);

		// 指定mapper输出类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowBean.class);

		// 指定最终输出数据的KV类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);

		// 指定job的输入原始目录
//		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileInputFormat.setInputPaths(job, new Path("C:\\Users\\wqj\\Desktop\\11.txt"));

		// 指定job的输出结果
		FileOutputFormat.setOutputPath(job, new Path("C:\\Users\\wqj\\Desktop\\21.txt"));

		// 将job 中配置相关参数,以及job所有java类所在的jar包,提交给yarn运行
		// job.submit();

		boolean waitForCompletion = job.waitForCompletion(true);
		//判断是否成功
		System.exit(waitForCompletion?0:1);
	}

}

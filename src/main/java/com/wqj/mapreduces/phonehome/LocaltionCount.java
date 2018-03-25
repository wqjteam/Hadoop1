package com.wqj.mapreduces.phonehome;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LocaltionCount {
	public static void main(String[] args) throws Exception{
		Configuration configuration = new Configuration();

//		configuration.set("","");
		Job job = Job.getInstance(configuration);

		// job.setJar(jar);
		// 指定本程序所在jar的所在路进
		job.setJarByClass(LocaltionCount.class);

		// 指定本业务的job要是用的map和maprducer类
		job.setMapperClass(LocaltionMap.class);
		job.setReducerClass(LocaltionReducer.class);
		
		
		/**
		 * 因为 要分组地区的号码  所以要应用新的 partitioner 筛选器
		 * */
		job.setPartitionerClass(LocaltionPartioner.class);
		
		/**
		 *  指定一定数量的reducer  几个地区 就有几个reducer
		 * */
		job.setNumReduceTasks(7);

		// 指定mapper输出类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LocaltionBean.class);

		// 指定最终输出数据的KV类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LocaltionBean.class);

		// 指定job的输入原始目录
		FileInputFormat.setInputPaths(job, new Path(args[0]));

		// 指定job的输出结果
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// 将job 中配置相关参数,以及job所有java类所在的jar包,提交给yarn运行
		// job.submit();

		boolean waitForCompletion = job.waitForCompletion(true);
		//判断是否成功
		System.exit(waitForCompletion?0:1);
	}

}

package com.wqj.mapreduces.phoneflow;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.wqj.mapreduces.numbercount.MapDemo;
import com.wqj.mapreduces.numbercount.ReducesDemo;
import com.wqj.mapreduces.numbercount.WordCountDriver;

public class FlowMap extends Mapper<LongWritable, Text, Text, FlowBean> {

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FlowBean>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String line = value.toString();
		
		String[] fields =line.split("\t");
		
		String phoneNBr = fields[1];
		
		//去除上行流量 下行流量
		Long upFlow = Long.parseLong(fields[fields.length-3]);
		
		Long dfLow = Long.parseLong(fields[fields.length-2]);
		
		Long sumflow = upFlow+dfLow;
		
		context.write(new Text(phoneNBr), new FlowBean(upFlow,dfLow,sumflow));
		
	}

}

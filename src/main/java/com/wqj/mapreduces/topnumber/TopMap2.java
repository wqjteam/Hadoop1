package com.wqj.mapreduces.topnumber;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TopMap2 extends Mapper<LongWritable , Text, TopBean, Text >{

	public static TopBean bean = new TopBean();
	
	public static Text text = new Text();
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, TopBean, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		//拿到上一个的结果,已经是个手机总的流量结果
		String line = value.toString();
		String filds[] = line.split("/t");
		String phoneNbr = filds[0];
		
		Long upFlow =Long.parseLong(filds[1]) ;
		
		Long downFlow = Long.parseLong(filds[2]);
		
		bean.setTopBean(upFlow, downFlow, upFlow+downFlow);
		
		text.set(phoneNbr);
		context.write(bean,  text);
	}

}

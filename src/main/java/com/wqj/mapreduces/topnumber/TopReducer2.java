package com.wqj.mapreduces.topnumber;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopReducer2 extends Reducer<TopBean, Text, Text, TopBean> {

	@Override
	protected void reduce(TopBean arg0, Iterable<Text> arg1, Reducer<TopBean, Text, Text, TopBean>.Context arg2)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		//因为每个号码都是单个的,只有一个next,直接输出到 文本中
		arg2.write(arg1.iterator().next(), arg0);
	}

	

}

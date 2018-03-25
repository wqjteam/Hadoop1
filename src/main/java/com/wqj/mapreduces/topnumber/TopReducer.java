package com.wqj.mapreduces.topnumber;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopReducer extends Reducer<Text, TopBean, Text, TopBean> {

	@Override
	protected void reduce(Text arg0, Iterable<TopBean> arg1, Reducer<Text, TopBean, Text, TopBean>.Context arg2)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		TopBean newflowBean = new TopBean();

		for (TopBean flowBean : arg1) {
			newflowBean.setFlowup(newflowBean.getFlowup() + flowBean.getFlowup());
			newflowBean.setFlowdown(newflowBean.getFlowdown() + flowBean.getFlowdown());
		}

		arg2.write(arg0, newflowBean);
	}

}

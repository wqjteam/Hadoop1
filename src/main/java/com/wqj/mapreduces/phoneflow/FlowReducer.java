package com.wqj.mapreduces.phoneflow;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

	@Override
	protected void reduce(Text arg0, Iterable<FlowBean> arg1, Reducer<Text, FlowBean, Text, FlowBean>.Context arg2)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		FlowBean newflowBean = new FlowBean();

		for (FlowBean flowBean : arg1) {
			newflowBean.setFlowup(newflowBean.getFlowup() + flowBean.getFlowup());
			newflowBean.setFlowdown(newflowBean.getFlowdown() + flowBean.getFlowdown());
		}

		arg2.write(arg0, newflowBean);
	}

}

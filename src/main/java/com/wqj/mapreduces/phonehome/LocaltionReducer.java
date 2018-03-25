package com.wqj.mapreduces.phonehome;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LocaltionReducer extends Reducer<Text, LocaltionBean, Text, LocaltionBean> {

	@Override
	protected void reduce(Text arg0, Iterable<LocaltionBean> arg1, Reducer<Text, LocaltionBean, Text, LocaltionBean>.Context arg2)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		LocaltionBean newflowBean = new LocaltionBean();

		for (LocaltionBean flowBean : arg1) {
			newflowBean.setFlowup(newflowBean.getFlowup() + flowBean.getFlowup());
			newflowBean.setFlowdown(newflowBean.getFlowdown() + flowBean.getFlowdown());
		}

		arg2.write(arg0, newflowBean);
	}

}

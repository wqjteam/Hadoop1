package com.wqj.mapreduces.phoneflow;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class FlowBean implements Writable{

	
	private Long flowup;
	
	
	private Long flowdown;

	private Long sumFlow;

	public Long getFlowup() {
		return flowup;
	}


	/**
	 * 
	 */
	public FlowBean() {
		super();
		// TODO Auto-generated constructor stub
	}


	public void setFlowup(Long flowup) {
		this.flowup = flowup;
	}


	public Long getFlowdown() {
		return flowdown;
	}


	public void setFlowdown(Long flowdown) {
		this.flowdown = flowdown;
	}





	/**
	 * @param flowup
	 * @param flowdown
	 * @param sumFlow
	 */
	public FlowBean(Long flowup, Long flowdown, Long sumFlow) {
		super();
		this.flowup = flowup;
		this.flowdown = flowdown;
		this.sumFlow = sumFlow;
	}


	public Long getSumFlow() {
		return sumFlow;
	}


	public void setSumFlow(Long sumFlow) {
		this.sumFlow = sumFlow;
	}


	/**
	 * @Description: 反序列化
	 * author wqj
	 * @param arg0
	 * @throws IOException
	 */
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		flowup=arg0.readLong();
		flowdown=arg0.readLong();
		sumFlow=arg0.readLong();
	}


	/**
	 * @Description: 序列化
	 * author wqj
	 * @param arg0
	 * @throws IOException
	 */
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		arg0.writeLong(flowup);
		arg0.writeLong(flowdown);
		arg0.writeLong(sumFlow);
		
	}


	@Override
	public String toString() {
		return "FlowBean [flowup=" + flowup + ", flowdown=" + flowdown + ", sumFlow=" + sumFlow + "]";
	}
	
	
}

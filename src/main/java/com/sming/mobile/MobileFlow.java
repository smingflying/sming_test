package com.sming.mobile;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;

import org.apache.hadoop.io.Writable;


public class MobileFlow implements Writable{
	private long upFlow;
	private long downFlow;
	private long sumFlow;
	
	public long getUpFlow() {
		return upFlow;
	}
	public void setUpFlow(long upFlow) {
		this.upFlow = upFlow;
	}
	public long getDownFlow() {
		return downFlow;
	}
	public void setDownFlow(long downFlow) {
		this.downFlow = downFlow;
	}
	public MobileFlow() {
		super();
		// TODO Auto-generated constructor stub
	}
	public MobileFlow(long upFlow, long downFlow) {
		super();
		this.upFlow = upFlow;
		this.downFlow = downFlow;
		this.sumFlow=this.upFlow+this.downFlow;
	}
	public void write(DataOutput out) throws IOException {
		out.writeLong(upFlow);
		out.writeLong(downFlow);
		out.writeLong(sumFlow);
	}
	public void readFields(DataInput in) throws IOException {
		upFlow = in.readLong();
		downFlow = in.readLong();
		sumFlow=in.readLong();
	}
	
	
	@Override
	public String toString() {
		return "upFlow=" + upFlow + "\t downFlow=" + downFlow + "\t sumFlow=" + sumFlow;
	}

}

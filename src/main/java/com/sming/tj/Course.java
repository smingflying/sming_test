package com.sming.tj;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;


public class Course implements Writable {
	private String name;
	private  int count;
	private long sum;
	private long avg;
	private int coursecount;

	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public int getCount() {
		return count;
	}
	public void setCount(int count) {
		this.count = count;
	}
	public long getSum() {
		return sum;
	}
	public void setSum(long sum) {
		this.sum = sum;
	}
	public long getAvg() {
		return avg;
	}
	public void setAvg(long avg) {
		this.avg = avg;
	}
	public int getCoursecount() {
		return coursecount;
	}
	public void setCoursecount(int coursecount) {
		this.coursecount = coursecount;
	}
	
	
	public Course() {
		super();
		// TODO Auto-generated constructor stub
	}
	public Course(long sum, int coursecount,int count,String name) {
		super();
		this.count = count;
		this.sum = sum;
		this.avg = sum / coursecount;
		this.coursecount = coursecount;
		this.name=name;
	}
	public void write(DataOutput out) throws IOException {
		out.writeLong(avg);
		out.writeInt(count);
		out.writeLong(sum);
		out.writeInt(coursecount);
		out.writeUTF(name);
	}
	public void readFields(DataInput in) throws IOException {
		avg = in.readLong();
		count =in.readInt();
		sum = in.readLong();
		coursecount=in.readInt();
		name=in.readUTF();
		
	}
	@Override
	public String toString() {
		return "人数："+count+"\t" +"平均分："+avg+"\t"+"参加考试的人员"+name; 
	}
	
	
}

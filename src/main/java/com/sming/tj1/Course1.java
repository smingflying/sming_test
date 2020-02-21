package com.sming.tj1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class Course1 implements WritableComparable<Course1> {
	private String name;
	private double sum;
	private double avg; 
	private int count;
	private String courseName;
	
	

	public String getCourseName() {
		return courseName;
	}

	public void setCourseName(String courseName) {
		this.courseName = courseName;
	}

	public Course1() {
		super();
		// TODO Auto-generated constructor stub
	}

	public Course1(String courseName,String name, double sum,int count) {
		super();
		this.name = name;
		this.sum = sum;
		this.count=count;
		this.avg = sum/count;
		this.courseName=courseName;
	}

	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public double getSum() {
		return sum;
	}

	public void setSum(double sum) {
		this.sum = sum;
	}

	public double getAvg() {
		return avg;
	}

	public void setAvg(double avg) {
		this.avg = avg;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(name);
		out.writeInt(count);
		out.writeDouble(avg);
		out.writeDouble(sum);
		out.writeUTF(courseName);
		
	}

	public void readFields(DataInput in) throws IOException {
		name=in.readUTF();
		count=in.readInt();
		avg=in.readDouble();
		sum=in.readDouble();
		courseName=in.readUTF();
		
	}

	public int compareTo(Course1 o) {
		
		return this.avg-o.getAvg()>0?-1:1;
	}

	@Override
	public String toString() {
		return "课程："+courseName+"\t"+"学生："+name+"\t"+"平均分："+avg;
	}
	
}

package com.sming.tj2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class Student implements WritableComparable<Student>{
	private String name;
	private String cname;
	private double max;
	private double avg;
	
	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getCname() {
		return cname;
	}

	public void setCname(String cname) {
		this.cname = cname;
	}

	public double getMax() {
		return max;
	}

	public void setMax(double max) {
		this.max = max;
	}

	public double getAvg() {
		return avg;
	}

	public void setAvg(double avg) {
		this.avg = avg;
	}
	

	public Student() {
		super();
		// TODO Auto-generated constructor stub
	}

	public Student(String name, String cname, double max,double avg) {
		super();
		this.name = name;
		this.cname = cname;
		this.max = max;
		this.avg = avg;
	}

	@Override
	public String toString() {
		return cname+"\t"+name+"\t"+avg+"\t"+max;
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(name);
		out.writeUTF(cname);
		out.writeDouble(avg);
		out.writeDouble(max);
	}

	public void readFields(DataInput in) throws IOException {
		name = in.readUTF();
		cname = in.readUTF();
		max = in.readDouble();
		avg = in.readDouble();
	}

	public int compareTo(Student cs) {
		int courseDiff = this.cname.compareTo(cs.getCname());

		if (courseDiff == 0) {
			double diff = cs.getAvg() - this.avg;
			if (diff == 0) {
				return 0;
			} else {
				return diff > 0 ? 1 : -1;
			}
		} else {
			return courseDiff > 0 ? 1 : -1;
		}
	}
}

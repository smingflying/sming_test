package com.sming.ser;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class Hadoop implements Writable {
	private String name;
	private Integer age;
	public Hadoop(String name, Integer age) {
		super();
		this.name = name;
		this.age = age;
	}
	public Hadoop() {
		super();
		// TODO Auto-generated constructor stub
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public Integer getAge() {
		return age;
	}
	public void setAge(Integer age) {
		this.age = age;
	}
	public void write(DataOutput out) throws IOException {
		out.writeUTF(name);
		out.writeInt(age);
		
	}
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		name = in.readUTF();
		age = in.readInt();
	}
	public static void main(String[] args) throws IOException {
		DataOutputStream dos = new DataOutputStream(new FileOutputStream("D:\\桌面\\a.txt"));
		new Hadoop("shimingming", 29).write(dos);
	}
}

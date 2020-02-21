package com.sming.temp;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class TempDate implements WritableComparable<TempDate> {
	private int year;
	private int month;
	private int day;
	private int temp;
	
	
	public int getYear() {
		return year;
	}

	public void setYear(int year) {
		this.year = year;
	}

	public int getMonth() {
		return month;
	}

	public void setMonth(int month) {
		this.month = month;
	}

	public int getDay() {
		return day;
	}

	public void setDay(int day) {
		this.day = day;
	}

	public int getTemp() {
		return temp;
	}

	public void setTemp(int temp) {
		this.temp = temp;
	}
	
	

	@Override
	public String toString() {
		return "TempDate [year=" + year + ", month=" + month + ", temp=" + temp + "]";
	}

	public TempDate(int year, int month, int day, int temp) {
		super();
		this.year = year;
		this.month = month;
		this.day = day;
		this.temp = temp;
	}

	public TempDate() {
		super();
		// TODO Auto-generated constructor stub
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(year);
		out.writeInt(month);
		out.writeInt(day);
		out.writeInt(temp);
		
	}

	public void readFields(DataInput in) throws IOException {
		year = in.readInt();
		month = in.readInt();
		day = in.readInt();
		temp = in.readInt();
		
	}

	public int compareTo(TempDate o) {
		int num = Integer.compare(this.year, o.getYear());
		if (num == 0) {
			int num1 = Integer.compare(this.month,o.getMonth());
			if (num1==0) {
				return -Integer.compare(this.temp,o.getTemp());
			}
			return num1;
		}
		return num;
	}
}

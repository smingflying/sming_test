package com.sming.temp;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MyCompare extends WritableComparator {
	public MyCompare() {
		super(TempDate.class,true);
	}
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		TempDate td1 = (TempDate) a;
		TempDate td2 = (TempDate) b;
		int num = Integer.compare(td1.getYear(),td2.getYear());
		if (num==0) {
			return Integer.compare(td1.getMonth(),td2.getMonth());
		}
		return num;
	}
}

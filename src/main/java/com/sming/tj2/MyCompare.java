package com.sming.tj2;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MyCompare extends WritableComparator{
	MyCompare(){
		super(Student.class,true);
	}
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		
		 Student cs1 = (Student)a;
         Student cs2 = (Student)b;

         int result = cs1.getCname().compareTo(cs2.getCname());
         return result;

	}
}

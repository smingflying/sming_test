package com.sming.tj2;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class StudentReduce extends Reducer<Student, NullWritable, Student, NullWritable> {
	int topNo = 1;
	@Override
	protected void reduce(Student student, Iterable<NullWritable> arg1, Context arg2)
			throws IOException, InterruptedException {
//		arg2.write(student, NullWritable.get());
		int num = 0 ;
		for (@SuppressWarnings("unused") NullWritable nullWritable : arg1) {
			arg2.write(student, NullWritable.get());
			num++;
			if (num==topNo) {
				break;
			}
		}
	}
}

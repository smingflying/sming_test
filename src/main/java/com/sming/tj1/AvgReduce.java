package com.sming.tj1;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class AvgReduce extends Reducer<Course1, NullWritable, Course1, NullWritable> {
	@Override
	protected void reduce(Course1 arg0, Iterable<NullWritable> arg1, Context arg2)
			throws IOException, InterruptedException {
		arg2.write(arg0, NullWritable.get());
	}
}

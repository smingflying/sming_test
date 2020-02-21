package com.sming.tj1;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class AvgPartition extends Partitioner<Course1, NullWritable> {

	@Override
	public int getPartition(Course1 key, NullWritable value, int numPartitions) {
		if ("math".equals(key.getCourseName())) {
			return 1;
		} else if ("english".equals(key.getCourseName())) {
			return 2;
		} else if ("computer".equals(key.getCourseName())) {
			return 3;
		} else if ("algorithm".equals(key.getCourseName())) {
			return 4;
		} else {
			return 0;
		}
	}

}

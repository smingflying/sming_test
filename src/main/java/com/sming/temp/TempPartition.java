package com.sming.temp;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class TempPartition extends Partitioner<TempDate, NullWritable> {

	@Override
	public int getPartition(TempDate key, NullWritable value, int numPartitions) {
		
		return key.getYear() % numPartitions;
	}

}

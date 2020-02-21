package com.sming.tj1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AvgMap extends Mapper<LongWritable, Text, Course1, NullWritable> {
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] split = value.toString().split(",");
		double sum = 0;
		int count=0;
		String reg = "^\\d+$";
		for (String string : split) {
			if (string.matches(reg)) {
				count++;
				sum += Double.parseDouble(string);
			}
		}
		context.write( new Course1(split[0],split[1],sum,count), NullWritable.get());
	}
}

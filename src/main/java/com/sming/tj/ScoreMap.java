package com.sming.tj;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ScoreMap extends Mapper<LongWritable, Text, Text, Course>{
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Course>.Context context)
			throws IOException, InterruptedException {
		String[] split = value.toString().split(",");
		String reg = "^\\d+$";
		long sum=0;
		for (String string : split) {
			if (string.matches(reg)) {
				sum += Long.parseLong(string);
			}
		}
		context.write(new Text(split[0]), new Course(sum, split.length-2,1,split[1]));
	}
}

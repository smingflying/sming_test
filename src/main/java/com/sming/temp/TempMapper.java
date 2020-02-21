package com.sming.temp;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TempMapper extends Mapper<LongWritable, Text, TempDate, NullWritable> {
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] split = value.toString().split("\\s+");
		String[] split2 = split[0].split("\\-");
		TempDate temp = new TempDate();
		temp.setYear(Integer.parseInt(split2[0]));
		temp.setMonth(Integer.parseInt(split2[1]));
		temp.setDay(Integer.parseInt(split2[2]));
		temp.setTemp(Integer.parseInt(split[2].substring(0, split[2].length()-1)));
		context.write(temp, NullWritable.get());
	}
}

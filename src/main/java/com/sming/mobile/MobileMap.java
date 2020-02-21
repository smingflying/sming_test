package com.sming.mobile;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MobileMap extends Mapper<LongWritable, Text, Text, MobileFlow> {
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String val = value.toString();
		String[] split = val.split("\t");

		MobileFlow mf = new MobileFlow();
		Text t = new Text();

		mf.setUpFlow(Long.valueOf(split[9]));
		mf.setDownFlow(Long.valueOf(split[10]));
		t.set(split[1]);
		context.write(t, mf);

	}
}

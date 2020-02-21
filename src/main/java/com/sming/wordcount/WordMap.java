package com.sming.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordMap extends Mapper<LongWritable, Text,Text, LongWritable>{
	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		String val = value.toString();
		String[] split = val.split("\\s");
		Text t = new Text();
		LongWritable lw = new LongWritable(1);
		for (String string : split) {
			t.set(string);
			context.write(t, lw);
		}
	}
	
}

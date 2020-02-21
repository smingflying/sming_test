package com.sming.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordReduce extends Reducer<Text, LongWritable, Text, LongWritable>{
	@Override
	protected void reduce(Text key, Iterable<LongWritable> value,
			Context context) throws IOException, InterruptedException {
		Long sum = 0l;
		for (LongWritable longWritable : value) {
			sum += longWritable.get();
		}
		context.write(key, new LongWritable(sum));
	}
}

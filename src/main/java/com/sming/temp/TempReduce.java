package com.sming.temp;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TempReduce extends Reducer<TempDate, NullWritable, Text, Text> {
	int count = 2;
	@SuppressWarnings("unused")
	@Override
	protected void reduce(TempDate key, Iterable<NullWritable> values, Context context)
			throws IOException, InterruptedException {
		int num=0;
		for (NullWritable nullWritable : values) {
			num++;
			String date = key.getYear()+"-"+key.getMonth()+"-"+key.getDay();
			String temp = key.getTemp()+"c";
			context.write(new Text(date), new Text(temp));
			if (num==count) {
				break;
			}
		}
	}
}

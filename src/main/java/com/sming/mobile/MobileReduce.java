package com.sming.mobile;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MobileReduce extends Reducer<Text, MobileFlow, Text, MobileFlow> {
	@Override
	protected void reduce(Text key, Iterable<MobileFlow> values,Context context) throws IOException, InterruptedException {
		Long upFlow=0l;
		Long downFlow=0l;
		for (MobileFlow mobileFlow : values) {
			upFlow += mobileFlow.getUpFlow();
			downFlow += mobileFlow.getDownFlow();
		}
		context.write(key, new MobileFlow(upFlow, downFlow));
	}
}

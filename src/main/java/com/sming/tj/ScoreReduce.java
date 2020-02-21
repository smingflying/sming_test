package com.sming.tj;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ScoreReduce extends Reducer<Text, Course, Text, Course> {
	@Override
	protected void reduce(Text key, Iterable<Course> values, Context context) throws IOException, InterruptedException {
		long sum = 0;
		int count = 0;
		int coursecount = 0;
		String name="";
		for (Course course : values) {
			sum += course.getSum();
			coursecount += course.getCoursecount();
			count += course.getCount();
			name += ","+course.getName();
			
		}
		name = name.substring(1);
		context.write(key, new Course(sum, coursecount, count,name ));
	}
}

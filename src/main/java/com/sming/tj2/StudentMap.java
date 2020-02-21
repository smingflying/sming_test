package com.sming.tj2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StudentMap extends Mapper<LongWritable, Text, Student, NullWritable>{
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] split = value.toString().split(",");
		double sum = 0;
		double max = 0;
		
		for (int i = 2; i < split.length; i++) {
			max=max>Double.parseDouble(split[i])?max:Double.parseDouble(split[i]);
			sum+=Double.valueOf(split[i]);
		}
		double avg = sum/(split.length-2);
		Student student = new Student(split[1],split[0],max,avg);
		context.write(student,NullWritable.get() );
	}
}

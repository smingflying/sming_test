package com.sming.tj2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StudentJob {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Path in = new Path("D:\\桌面\\mobileflow.log");
		Path out = new Path("D:\\桌面\\out");
		Configuration conf = new Configuration();
		out.getFileSystem(conf).delete(out, true);
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(StudentJob.class);
		job.setMapperClass(StudentMap.class);
		job.setMapOutputKeyClass(Student.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setReducerClass(StudentReduce.class);
		job.setOutputKeyClass(Student.class);
		job.setOutputValueClass(NullWritable.class);
		job.setGroupingComparatorClass(MyCompare.class);
		FileInputFormat.setInputPaths(job, in);;
		FileOutputFormat.setOutputPath(job, out);
		
		boolean waitForCompletion = job.waitForCompletion(true);
		System.exit(waitForCompletion?0:1);
	}
}

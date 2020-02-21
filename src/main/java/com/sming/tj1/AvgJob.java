package com.sming.tj1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class AvgJob {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Path in = new Path(args[0]);
		Path out = new Path(args[1]);
		Configuration conf = new Configuration();
		out.getFileSystem(conf).delete(out, true);
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(AvgJob.class);
		job.setMapperClass(AvgMap.class);
		job.setMapOutputKeyClass(Course1.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setReducerClass(AvgReduce.class);
		job.setOutputKeyClass(Course1.class);
		job.setOutputValueClass(NullWritable.class);
		job.setPartitionerClass(AvgPartition.class);
		job.setNumReduceTasks(5);
		FileInputFormat.setInputPaths(job, in);;
		FileOutputFormat.setOutputPath(job, out);
		
		boolean waitForCompletion = job.waitForCompletion(true);
		System.exit(waitForCompletion?0:1);
	}
}

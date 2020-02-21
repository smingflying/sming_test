package com.sming.temp;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TempJob {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Path in = new Path("hdfs://hadoop:9000/temp.txt");
		Path out = new Path("hdfs://hadoop:9000/out");
		Configuration conf = new  Configuration();
		boolean exists = out.getFileSystem(conf).exists(out);
		if (exists) {
			out.getFileSystem(conf).delete(out,true);
		}
		Job job = Job.getInstance(conf, TempJob.class.getSimpleName());
		job.setJarByClass(TempJob.class);
		job.setMapperClass(TempMapper.class);
		job.setMapOutputKeyClass(TempDate.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setReducerClass(TempReduce.class);
		job.setOutputKeyClass(TempDate.class);
		job.setOutputValueClass(NullWritable.class);
		job.setGroupingComparatorClass(MyCompare.class);
		job.setSortComparatorClass(MyCompare.class);
		job.setPartitionerClass(TempPartition.class);
		job.setNumReduceTasks(4);
		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);
		boolean waitForCompletion = job.waitForCompletion(true);
		System.exit(waitForCompletion?0:1);
	}
}

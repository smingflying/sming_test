package com.sming.mobile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MobileJob {
	public static void main(String[] args) throws Exception {
		Path in = new Path("hdfs://192.168.230.132:9000/mobile.log");
		Path out = new Path("hdfs://192.168.230.132:9000/out");
		Configuration conf = new Configuration();
		out.getFileSystem(conf).delete(out, true);
		Job job = Job.getInstance(conf);
		job.setJarByClass(MobileJob.class);
		job.setMapperClass(MobileMap.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(MobileFlow.class);
		
		job.setReducerClass(MobileReduce.class);
		job.setPartitionerClass(MobilePartion.class);
		job.setNumReduceTasks(4);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(MobileFlow.class);
		
		FileInputFormat.setInputPaths(job, in);;
		FileOutputFormat.setOutputPath(job, out);
	
		boolean waitForCompletion = job.waitForCompletion(true);
		System.exit(waitForCompletion?0:1);
	}
}

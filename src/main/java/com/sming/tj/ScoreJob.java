package com.sming.tj;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class ScoreJob {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Path in = new Path(args[0]);
		Path out = new Path(args[1]);
		Configuration conf = new Configuration();
		out.getFileSystem(conf).delete(out, true);
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(ScoreJob.class);
		job.setMapperClass(ScoreMap.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Course.class);
		
		job.setReducerClass(ScoreReduce.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Course.class);
		
		FileInputFormat.setInputPaths(job, in);;
		FileOutputFormat.setOutputPath(job, out);
		
		boolean waitForCompletion = job.waitForCompletion(true);
		System.exit(waitForCompletion?0:1);
	}
}

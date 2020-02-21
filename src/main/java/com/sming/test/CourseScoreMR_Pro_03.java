package com.sming.test;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
 
import java.io.IOException;
 
public class CourseScoreMR_Pro_03 {
 
    public static void main(String[] args) throws Exception {
      
        /**
         * 初始化一个Job对象
         */
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
 
        /**
         * 设置jar包所在路径
         */
        job.setJarByClass(CourseScoreMR_Pro_03.class);
 
        /**
         * 指定mapper类和reducer类 等各种其他业务逻辑组件
         */
        job.setMapperClass(Mapper_CS.class);
        job.setReducerClass(Reducer_CS.class);
        // 指定maptask的输出类型
        job.setMapOutputKeyClass(CourseScore.class);
        job.setMapOutputValueClass(NullWritable.class);
        // 指定reducetask的输出类型
        job.setOutputKeyClass(CourseScore.class);
        job.setOutputValueClass(NullWritable.class);
 
        job.setGroupingComparatorClass(CourseScoreGroupComparator.class);
 
        /**
         * 指定该mapreduce程序数据的输入和输出路径
         */
        Path input = new Path(args[0]);
        Path output = new Path(args[1]);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(output)) {
            fs.delete(output, true);
        }
        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, output);
 
        /**
         * 最后提交任务
         */
        boolean waitForCompletion = job.waitForCompletion(true);
        System.exit(waitForCompletion ? 0 : 1);
    }
 
    /**
     * Mapper组件：
     * <p>
     * 输入的key:
     * 输入的value: computer,xuzheng,54,52,86,91,42
     * <p>
     * 输出的key: CourseScore
     * 输入的value: NullWritable
     */
    private static class Mapper_CS extends Mapper<LongWritable, Text, CourseScore, NullWritable> {
 
        CourseScore keyOut = new CourseScore();
 
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
 
            String[] splits = value.toString().split(",");
            String course = splits[0];
            String name = splits[1];
 
            int sum = 0;
            int num = 0;
            for(int i=2; i<splits.length; i++){
                sum += Integer.valueOf(splits[i]);
                num ++;
            }
            double avgScore = Math.round(sum * 1D / num * 10) / 10D;
 
            keyOut.setCourse(course);
            keyOut.setName(name);
            keyOut.setScore(avgScore);
 
            context.write(keyOut, NullWritable.get());
        }
    }
 
    /**
     * Reducer组件：
     * <p>
     * 输入的key: CourseScore
     * 输入的values: NullWritable
     * <p>
     * 输出的key: CourseScore
     * 输入的value: NullWritable
     */
    private static class Reducer_CS extends Reducer<CourseScore, NullWritable, CourseScore, NullWritable> {
 
        // 成绩最高的两个人的信息
        int topN = 2;
 
        @Override
        protected void reduce(CourseScore key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
 
            int number = 0;
            for(NullWritable nvl: values){
                context.write(key, nvl);
                number ++;
                if(number == topN){
                    break;
                }
            }
        }
    }
 
    /**
     * 自定义分组组件
     */
    public static class CourseScoreGroupComparator extends WritableComparator{
 
        CourseScoreGroupComparator(){
            super(CourseScore.class, true);
        }
 
        @SuppressWarnings("rawtypes")
		@Override
        public int compare(WritableComparable a, WritableComparable b) {
 
            CourseScore cs1 = (CourseScore)a;
            CourseScore cs2 = (CourseScore)b;
 
            int result = cs1.getCourse().compareTo(cs2.getCourse());
            return result;
        }
    }
}

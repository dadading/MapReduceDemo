package com.dingguanyi.Etldemo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WeblogDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1.创建job实体,配置信息
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //2.关联driver程序class
        job.setJarByClass(WeblogDriver.class);

        //3.关联map函数class
        job.setMapperClass(WeblogMapper.class);
        job.setNumReduceTasks(0);

        //4设置map函数输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        //5.设置最终输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //6.设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path("D:\\data\\explame\\inputlog"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\data\\WeblogDriver"));

        //提交job
        boolean result = job.waitForCompletion(true);
        System.out.println(result ? 0 : 1);
    }
}

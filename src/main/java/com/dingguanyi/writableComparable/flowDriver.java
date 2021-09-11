package com.dingguanyi.writableComparable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class flowDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1.获取配置信息以及获取job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //2.关联本Driver程序的jar
        job.setJarByClass(flowDriver.class);

        //3.关联Mapper和Reducer的jar
        job.setMapperClass(flowMapper.class);
        job.setReducerClass(flowReduce.class);

        //4.设置Mapper输出的kv类型
        job.setMapOutputKeyClass(flowBean.class);
        job.setMapOutputValueClass(Text.class);

        //5.设置最终输出kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(flowBean.class);

        //6.设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path("D:\\data\\flowDriver"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\data\\comparable"));

        //7.提交job
        boolean result = job.waitForCompletion(true);
        System.out.println(result ? 0 : 1);
    }
}
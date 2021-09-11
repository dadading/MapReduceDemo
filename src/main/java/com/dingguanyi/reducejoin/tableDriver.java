package com.dingguanyi.reducejoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class tableDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1.获取配置信息以及获取job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //2.关联本Driver程序的jar
        job.setJarByClass(tableDriver.class);

        //3.关联Mapper和Reducer的jar
        job.setMapperClass(tableMapper.class);
        job.setReducerClass(tableReduce.class);

        //4.设置Mapper输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(tableBean.class);

        //5.设置最终输出kv类型
        job.setOutputKeyClass(tableBean.class);
        job.setOutputValueClass(NullWritable.class);

        //6.设置输入和输出路径
        FileInputFormat.setInputPaths(job,new Path("D:\\data\\explame\\inputtable"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\data\\tableDriver"));

        //7.提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}

package com.dingguanyi.combineTextInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        //1.获取配置信息以及获取job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //2.关联本Driver程序的jar
        job.setJarByClass(WordCountDriver.class);

        //3.关联Mapper和Reducer的jar
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReduce.class);

        //4.设置Mapper输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //5.设置最终输出kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //6.设置输入和输出路径
        FileInputFormat.setInputPaths(job,new Path("D:\\data\\explame\\inputcombinetextinputformat"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\data\\WordCountDriver"));

        //8.如果不设置InputFormat，他默认用的是TextInputFormat
        job.setInputFormatClass(CombineTextInputFormat.class);
        //虚拟存储切片最大值设置为4M
        CombineTextInputFormat.setMaxInputSplitSize(job,20971520);

        //7.提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }


}

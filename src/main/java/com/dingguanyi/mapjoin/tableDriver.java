package com.dingguanyi.mapjoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class tableDriver {
    public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
        //1.创建job和配置信息
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //2.设置驱动class
        job.setJarByClass(tableDriver.class);

        //3.设置mapclass
        job.setMapperClass(tableMapper.class);

        //4.设置map阶段输入和输出
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        //5.设置最终输出
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //6.加载缓存数据,取消reduce
        job.addCacheFile(new URI("file:///D:/data/explame/tablecache/pd.txt"));
        job.setNumReduceTasks(0);

        //7.设置输入输出路径
        FileInputFormat.setInputPaths(job,new Path("D:\\data\\explame\\inputtable2"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\data\\tableDriver"));

        //8.提交job
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}

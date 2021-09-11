package com.dingguanyi.mapjoin;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

public class tableMapper extends Mapper<LongWritable, Text,Text, NullWritable> {
    //定义map集合存储小表信息
    private HashMap<String,String> pdMap = new HashMap<String,String>();
    private Text outK = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //通过缓存文件得到小表数据
        URI[] cacheFiles = context.getCacheFiles();
        Path path = new Path(cacheFiles[0]);

        //获取文件系统对象,并开流
        FileSystem fs = FileSystem.get(context.getConfiguration());
        FSDataInputStream fis = fs.open(path);

        //通过包装流转换成reader
        BufferedReader reader = new BufferedReader(new InputStreamReader(fis,"UTF-8"));

        //按行读取处理
        String line;
        while(StringUtils.isNotEmpty(line=reader.readLine())){
            String[] split = line.split("\t");
            pdMap.put(split[0],split[1]);
        }

        //关闭流
        IOUtils.closeStream(reader);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //读取大表
        String[] split = value.toString().split("\t");

        //通过大表中的pid去pdMap中去取pname
        String pname = pdMap.get(split[1]);

        //封转key
        outK.set(split[0]+"\t"+pname+"\t"+split[2]);

        //输出map结果
        context.write(outK,NullWritable.get());
    }
}

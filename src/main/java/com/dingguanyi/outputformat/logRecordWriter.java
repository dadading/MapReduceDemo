package com.dingguanyi.outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class logRecordWriter extends RecordWriter<Text, NullWritable> {

    private  FSDataOutputStream atguiguOut;
    private  FSDataOutputStream otherOut;

    public logRecordWriter(TaskAttemptContext job) {
        try {
            //获取文件系统
            FileSystem fs = FileSystem.get(job.getConfiguration());
            //用文件系统对象创建两个不同的流
            atguiguOut = fs.create(new Path("D:\\data\\atguiguOut\\atguigu.txt"));
            otherOut = fs.create(new Path("D:\\data\\otherOut\\other.txt"));
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        String log = key.toString();
        //根据log是否包含atguigu
        if(log.contains("atguigu")){
            atguiguOut.writeBytes(log+"\n");
        }else{
            otherOut.writeBytes(log+"\n");
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        //关闭数据流
        IOUtils.closeStream(atguiguOut);
        IOUtils.closeStream(otherOut);
    }
}

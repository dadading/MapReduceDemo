package com.dingguanyi.partitionerandComparable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class flowMapper extends Mapper<LongWritable,Text, flowBean, Text> {
    flowBean outK = new flowBean();
    Text outV = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] col = value.toString().split("\t");

        //获取输出key/value信息
        outK.setUpFlow(Integer.parseInt(col[1]));
        outK.setDownFlow(Integer.parseInt(col[2]));
        outK.setTotalFlow();
        outV.set(col[0]);

        //map函数输出
        context.write(outK,outV);
    }
}

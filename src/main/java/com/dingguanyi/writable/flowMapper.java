package com.dingguanyi.writable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import javax.xml.transform.OutputKeys;
import java.io.IOException;

public class flowMapper extends Mapper<LongWritable,Text, Text,flowBean> {
    flowBean fb = new flowBean();
    Text outK = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //将输入的value转化为字符串数组
        String line = value.toString();
        String[] colList = line.split("\t");

        //获取phone/upFlow/downFlow信息
        String phone = colList[1];
        String upFlow = colList[colList.length-3];
        String downFlow = colList[colList.length-2];

        //封装输出key/value
        fb.setUpFlow(Integer.parseInt(upFlow));
        fb.setDownFlow(Integer.parseInt(downFlow));
        fb.setTotalFlow();
        outK.set(phone);

        //输出mapper结果
        context.write(outK,fb);
    }
}

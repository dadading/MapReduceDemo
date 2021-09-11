package com.dingguanyi.writableComparable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class flowReduce extends Reducer<flowBean, Text, Text, flowBean> {

    @Override
    protected void reduce(flowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //遍历values集合,循环写出,避免总流量相同的情况
        for (Text value : values) {
            //反向写出
            context.write(value,key);
        }
    }
}

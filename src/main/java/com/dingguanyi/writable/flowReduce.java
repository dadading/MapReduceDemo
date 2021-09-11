package com.dingguanyi.writable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class flowReduce extends Reducer<Text, flowBean, Text, flowBean> {
    flowBean fb = new flowBean();

    @Override
    protected void reduce(Text key, Iterable<flowBean> values, Context context) throws IOException, InterruptedException {
        //将相同key的信息累加
        int upFlow = 0;
        int downFlow = 0;

        for (flowBean value : values) {
            upFlow += value.getUpFlow();
            downFlow += value.getDownFlow();
        }

        //封转输出value
        fb.setUpFlow(upFlow);
        fb.setDownFlow(downFlow);
        fb.setTotalFlow();

        //写出reduce结果
        context.write(key, fb);
    }
}

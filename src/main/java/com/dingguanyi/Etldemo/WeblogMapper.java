package com.dingguanyi.Etldemo;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WeblogMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1.获取一行内容
        String line = value.toString();

        //2.ETL规则
        boolean result = parseLog(line, context);

        //3.如果日志不合法直接返回
        if (!result) {
            return;
        }

        //4.输出map
        context.write(value, NullWritable.get());
    }

    private boolean parseLog(String line, Context context) {
        String[] split = line.split(" ");

        if (split.length > 11) {
            return true;
        } else {
            return false;
        }
    }
}

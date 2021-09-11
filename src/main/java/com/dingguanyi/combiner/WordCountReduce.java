package com.dingguanyi.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * KEYIN reduce阶段输入key类型 Text
 * VALUEIN reduce阶段输入value类型 IntWritable
 * KEYOUT reduce阶段输出key类型 Text
 * VALUEOUT reduce阶段输出value类型 IntWritable
 *
 */

public class WordCountReduce extends Reducer<Text,IntWritable,Text, IntWritable> {
    //放在reduce方法外,减少内存开销
    IntWritable v  = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //每次调用reduce方法,需要将sum重置为0,重新累加求和
        int sum=0;

        //累计求和
        for (IntWritable value : values) {
            sum+=value.get();
        }
        v.set(sum);

        //reduce函数输出
        context.write(key,v);
    }
}

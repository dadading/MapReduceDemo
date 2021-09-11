package com.dingguanyi.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * KEYIN mapper阶段输入key类型 LongWritable
 * VALUEIN mapper阶段输入value类型 Text
 * KEYOUT mapper阶段输出key类型 Text
 * VALUEOUT mapper阶段输出value类型 IntWritable
 *
 */

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    //放在map方法外,减少内存开销
    Text k = new Text();
    IntWritable v = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //将每行String内容按空格分割成为String数组
        String[] words = value.toString().split(" ");

        //循环数组将每个word拼接1组合k,v输出
        for (String word : words) {
            k.set(word);

            //map函数的输出
            context.write(k,v);
        }
    }
}

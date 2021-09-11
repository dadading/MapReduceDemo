package com.dingguanyi.reducejoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class tableMapper extends Mapper<LongWritable, Text,Text,tableBean> {

    private String filename;
    private Text outK = new Text();
    private tableBean outV = new tableBean();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit split = (FileSplit) context.getInputSplit();
        filename = split.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取一行
        String line = value.toString();

        //封转输出key/value
        if(filename.contains("order")){
            //针对不同输入文件,有不同的处理,例如分隔符等
            //订单表
            //id	pid	amount
            //1001	01	1
            String[] col = line.split("\t");
            outK.set(col[1]);
            outV.setId(col[0]);
            outV.setPid(col[1]);
            outV.setAmount(Integer.parseInt(col[2]));
            outV.setPname("");
            outV.setFlag("order");
        }else{
            //针对不同输入文件,有不同的处理,例如分隔符等
            //产品表
            //pid	pname
            //01	小米
            String[] col = line.split("\t");
            outK.set(col[0]);
            outV.setId("");
            outV.setPid(col[0]);
            outV.setAmount(0);
            outV.setPname(col[1]);
            outV.setFlag("pd");
        }

        //map输出key/value
        context.write(outK,outV);
    }
}

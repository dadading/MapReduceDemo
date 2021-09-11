package com.dingguanyi.partioner2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

//由于分区是在map函数之后,reduce函数之前,所以出入类型就是map函数输出类型
public class ProvincePartitioner extends Partitioner<Text, flowBean> {
    @Override
    public int getPartition(Text text, flowBean flowBean, int numPartitions) {
        String prePhone = text.toString().substring(0, 3);
        int partition;

        //常量放在前,可以防止空指针
        if("136".equals(prePhone)){
            partition=0;
        }else if("137".equals(prePhone)){
            partition=1;
        }else if("138".equals(prePhone)){
            partition=2;
        }else if("139".equals(prePhone)){
            partition=3;
        }else {
            partition=4;
        }

        return partition;
    }
}

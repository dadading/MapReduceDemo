package com.dingguanyi.reducejoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

public class tableReduce extends Reducer<Text, tableBean, tableBean, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<tableBean> values, Context context) throws IOException, InterruptedException {
        ArrayList<tableBean> orderBeans = new ArrayList<>();
        tableBean pdBean = new tableBean();

        for (tableBean value : values) {
            if ("order".equals(value.getFlag())) {//订单表
                //创建临时tableBean对象来接受value
                tableBean tmporderBean = new tableBean();

                try {
                    BeanUtils.copyProperties(tmporderBean,value);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }

                //将对象添加到list中
                orderBeans.add(tmporderBean);
            }else{//产品表
                try {
                    BeanUtils.copyProperties(pdBean,value);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            }
        }
        
        //循环给Pname赋值,并写出结果
        for (tableBean tableBean : orderBeans) {
            tableBean.setPname(pdBean.getPname());
            context.write(tableBean,NullWritable.get());
        }
    }
}

package com.dingguanyi.partitionerandComparable;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * hadoop序列化的优点
 * 1.紧凑：高效使用存储空间
 * 2.快速：读写数据的额外开销小
 * 3.互操作：支持多语言的交互
 * <p>
 * hadoop实现bean对象实例化步骤如下：
 * 1.必须实现writable接口
 * 2.反序列化时，需要反射调用空参构造函数，所以必须有空参构造
 * 3.重写序列化方法
 * 4.重写反序列化方法
 * 5.注意反序列化的顺序和序列化的顺序完全一致
 * 6.要想把结果显示在文件中，需要重写toString()方法，可以使用\t分割，方便后续使用
 * 7.如果需要将自定义的bean放在key中传输，则还需要实现Comparable接口
 */

public class flowBean implements WritableComparable<flowBean> {
    private int upFlow;
    private int downFlow;
    private int totalFlow;

    public flowBean() {
    }

    public int getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(int upFlow) {
        this.upFlow = upFlow;
    }

    public int getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(int downFlow) {
        this.downFlow = downFlow;
    }

    public int getTotalFlow() {
        return totalFlow;
    }

    public void setTotalFlow(int totalFlow) {
        this.totalFlow = totalFlow;
    }

    //重载setTotalFlow()方法
    public void setTotalFlow() {
        this.totalFlow = this.upFlow + this.downFlow;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(upFlow);
        out.writeInt(downFlow);
        out.writeInt(totalFlow);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.upFlow = in.readInt();
        this.downFlow = in.readInt();
        this.totalFlow = in.readInt();
    }

    @Override
    public String toString() {
        return upFlow + "\t" + downFlow + "\t" + totalFlow;
    }


    @Override
    public int compareTo(flowBean o) {
        if(this.totalFlow>o.totalFlow){
            return -1;
        }else if(this.totalFlow<o.totalFlow){
            return 1;
        }else{
            //二次排序
            if(this.upFlow>o.upFlow){
                return 1;
            }else if(this.upFlow<o.upFlow){
                return -1;
            }else{
                return 0;
            }
        }
    }
}

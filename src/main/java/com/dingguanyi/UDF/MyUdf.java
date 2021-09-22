package com.dingguanyi.UDF;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * 自定义UDF函数需要继承GenericUDF函数
 * 需求：计算指定字符串的长度
 *
 */


public class MyUdf extends GenericUDF {

    /**
     *
     * @param objectInspectors 输入参数类型的鉴别器对象
     * @return 返回值类型的鉴别器对象
     * @throws UDFArgumentException
     */
    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        //判断输入参数的个数
        if(objectInspectors.length!=1){
            throw  new UDFArgumentLengthException("输入参数个数错误！");
        }

        //判断输入参数的类型
        if(!objectInspectors[0].getCategory().equals(ObjectInspector.Category.PRIMITIVE)){
            throw new UDFArgumentTypeException(0,"输入参数类型错误！");
        }

        //函数本身返回的是int类型,需要返回int类型的鉴别器
        return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
    }

    /**
     *
     * @param deferredObjects 输入参数
     * @return 返回值
     * @throws HiveException
     */
    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        if(deferredObjects[0]==null){
            return 0;
        }else{
            return deferredObjects[0].get().toString().length();
        }
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "MyUdf";
    }


}

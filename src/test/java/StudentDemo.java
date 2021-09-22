import com.google.common.hash.HashCode;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.Writable;

public class StudentDemo {
    public static void main(String[] args) {
//        String s = "9 \t13729199489\t192.168.100.6\t\t\t240\t0\t200";
//
//        String[] ss = s.split("\t");
//        System.out.println(ss[1]);
//        System.out.println(ss[4]);
//        System.out.println(ss[5]);
//
//        String a = ss[4];
//        System.out.println("a:" + a);
//        Long upFlow = Long.parseLong(a);

        String bb = "10";
        System.out.println(bb.hashCode()%3);

        Integer i1 = new Integer(10);
        Integer i2 = new Integer(20);
        Integer i3 = new Integer(30);
        System.out.println("10:"+i1.hashCode()%4);
        System.out.println("20:"+i2.hashCode()%4);
        System.out.println("30:"+i3.hashCode()%4);

        System.out.println(ObjectInspector.Category.PRIMITIVE);
    }
}

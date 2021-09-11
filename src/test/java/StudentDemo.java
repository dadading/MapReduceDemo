import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.Writable;

public class StudentDemo {
    public static void main(String[] args) {
        String s = "9 \t13729199489\t192.168.100.6\t\t\t240\t0\t200";

        String[] ss = s.split("\t");
        System.out.println(ss[1]);
        System.out.println(ss[4]);
        System.out.println(ss[5]);

        String a = ss[4];
        System.out.println("a:" + a);
        Long upFlow = Long.parseLong(a);
    }
}

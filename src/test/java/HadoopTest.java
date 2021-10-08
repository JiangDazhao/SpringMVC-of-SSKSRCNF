import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HadoopTest {
    public static void main(String[] args) throws IOException, InterruptedException {

        //获取文件系统
        Configuration conf=new Configuration();
        FileSystem fs=FileSystem.get(conf);
        // 2 创建输入流；路径前不需要加file:///，否则报错
        FileInputStream fis = new FileInputStream(new File("/home/jiangxuzhao/Desktop/Data/uploadtest"));
        // 3 创建输出流
        FSDataOutputStream fos = fs.create(new Path("hdfs://master:9000/uploadtest"),
                true,4096,(short)1,496);
        // 4 流对拷 org.apache.commons.io.IOUtils
        IOUtils.copy(fis,fos);
        // 5 关闭资源
        IOUtils.closeQuietly(fos);
        IOUtils.closeQuietly(fis);
        fs.close();
    }
}
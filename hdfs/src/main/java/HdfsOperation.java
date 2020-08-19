import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class HdfsOperation {
    /**
     * 在HDFS中创建一个文件夹
     * @param path 待创建的路径名称
     * @throws IOException 如果未能正确获取FileSystem信息就会抛出IOException.
     */
    public static void mkdir(String path) throws IOException {
        //建立一个hadoop的配置项
        Configuration configuration = new Configuration();

        //设置configuration选项，这里的fs.defaultFS及其值都与core-site.xml中的一致
        configuration.set("fs.defaultFS", "hdfs://knot1:8020");

        //实例化一个FileSystem对象，用于获取hdfs的信息
        FileSystem fileSystem = FileSystem.get(configuration);

        Path p = new Path(path);

        //判断路径是否存在，存在就不再创建了
        if(fileSystem.exists(p)) {
            System.out.println("This directory already exists.");
            fileSystem.close();
            return;
        }

        //新建文件（可以是多级文件夹）
        fileSystem.mkdirs(p);

        //最后关闭文件系统的连接
        fileSystem.close();
    }

    /**
     * 将本地文件复制到HDFS上
     * @param src 源文件路径
     * @param dest 目标文件路径
     * @throws IOException 当文件系统操作出现异常时抛出
     */
    public static void uploadFile(String src, String dest) throws IOException {
        //建立一个hadoop的配置项
        Configuration configuration = new Configuration();

        //设置configuration选项
        configuration.set("fs.defaultFS", "hdfs://knot1:8020");
        //下面这个配置很重要，为了能让此程序（外网）访问到datanode
        configuration.set("dfs.client.use.datanode.hostname", "true");

        FileSystem fileSystem = FileSystem.get(configuration);
        Path srcPath = new Path(src);
        Path destPath = new Path(dest);

        //这里不再判断文件是否存在，当文件存在时，覆盖之
        fileSystem.copyFromLocalFile(false, true, srcPath, destPath);
        fileSystem.close();
    }
}

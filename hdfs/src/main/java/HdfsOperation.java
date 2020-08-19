import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class HdfsOperation {
    //创建文件夹
    public static void mkdir(String path) throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://knot1:8020");
        FileSystem fileSystem = FileSystem.get(configuration);
        fileSystem.mkdirs(new Path(path));
        fileSystem.close();
    }

    //上传文件
}

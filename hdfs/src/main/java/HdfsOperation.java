import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HdfsOperation {
    /**
     * 在HDFS中创建一个文件夹
     *
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
            System.out.println("此路径已经存在");
            fileSystem.close();
            return;
        }

        //新建文件（可以是多级文件夹）
        if(fileSystem.mkdirs(p)) {
            System.out.println("创建成功");
        }

        //最后关闭文件系统的连接
        fileSystem.close();
    }

    /**
     * 将本地文件复制到HDFS上
     *
     * @param src  源文件路径
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

    /**
     * 将HDFS上的文件复制到本地目录
     *
     * @param src  源文件路径
     * @param dest 目标文件的本地路径
     * @throws IOException 当文件系统操作出现异常时，抛出
     */
    public static void downloadFile(String src, String dest) throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://knot1:8020");
        //下面这个配置很重要，为了能让此程序（外网）访问到datanode
        configuration.set("dfs.client.use.datanode.hostname", "true");

        FileSystem fileSystem = FileSystem.get(configuration);
        Path srcPath = new Path(src);
        Path destPath = new Path(dest);

        if(!fileSystem.exists(srcPath)) {
            System.out.println("源文件不存在");
            fileSystem.close();
            return;
        }

        fileSystem.copyToLocalFile(srcPath, destPath);
        fileSystem.close();
    }

    /**
     * 删除指定文件及其下子文件
     *
     * @param dest 目标文件
     * @throws IOException 抛出异常
     */
    public static void deleteFile(String dest) throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://knot1:8020");
        //这里由于不需要和datanode通信，因此不需加下面的配置
        //configuration.set("dfs.client.use.datanode.hostname", "true");

        FileSystem fileSystem = FileSystem.get(configuration);
        Path destPath = new Path(dest);

        if(!fileSystem.exists(destPath)) {
            System.out.println("文件不存在");
            fileSystem.close();
            return;
        }

        //递归地删除该路径下的子文件（夹）
        if(fileSystem.delete(destPath, true)) {
            System.out.println("删除成功");
        }
        fileSystem.close();
    }

    /**
     * 重命名指定文件
     *
     * @param src  源文件
     * @param dest 目标文件
     * @throws IOException 抛出异常
     */
    public static void renameFile(String src, String dest) throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://knot1:8020");
        //这里由于不需要和datanode通信，因此不需加下面的配置
        //configuration.set("dfs.client.use.datanode.hostname", "true");

        FileSystem fileSystem = FileSystem.get(configuration);

        Path srcPath = new Path(src);
        Path destPath = new Path(dest);

        if(!fileSystem.exists(srcPath)) {
            System.out.println("源文件不存在");
            fileSystem.close();
            return;
        }

        if(fileSystem.rename(srcPath, destPath)) {
            System.out.println("重命名成功");
        }
        fileSystem.close();
    }

    public static void listFiles(String dest) throws IOException, URISyntaxException {
        //获取文件系统
        Configuration configuration = new Configuration();
        //另一种获取FileSystem的方式，具体见get的方法说明
        FileSystem fileSystem = FileSystem.get(
                new URI("hdfs://knot1:8020"),
                configuration);

        //获取文件及其子文件状态
        //RemoteIterator作为返回信息的接收类型
        RemoteIterator<LocatedFileStatus> listFiles = fileSystem.listFiles(new Path(dest), true);

        //遍历并打印所有文件的指定状态
        while(listFiles.hasNext()) {
            LocatedFileStatus status = listFiles.next();

            System.out.print(status.getPath().getName() + "\t");
            System.out.print(status.getLen() + "B\t");
            System.out.print(status.getPermission() + "\t");
            System.out.print(status.getGroup() + "\t@{");
            BlockLocation[] blockLocations = status.getBlockLocations();
            for(BlockLocation blockLocation : blockLocations) {
                String[] hosts = blockLocation.getHosts();
                for(int i = 0; i < hosts.length; i++) {
                    System.out.print(hosts[i] + (i < hosts.length - 1 ? ", " : "}\n"));
                }
            }
        }

        fileSystem.close();
    }

    /**
     * 将文件通过IO流上传至HDFS
     *
     * @param filename 待读取的文件名（本地文件）
     * @param dest     目标地址（HDFS地址）
     * @throws IOException 抛出异常
     */
    public static void putFileToHDFS(String filename, String dest) throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://knot1:8020");
        configuration.set("dfs.client.use.datanode.hostname", "true");
        FileSystem fileSystem = FileSystem.get(configuration);

        //标准文件输入流
        FileInputStream fis = new FileInputStream(new File(filename));

        //HDFS文件输出流
        FSDataOutputStream fos = fileSystem.create(new Path(dest));

        byte[] block = new byte[1024];
        int len;
        while((len = fis.read(block)) != -1) {
            fos.write(block, 0, len);
            fos.flush();
        }
        fis.close();
        fos.close();
        fileSystem.close();
    }

    public static void getFileFromHDFS(String src, String filename) throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://knot1:8020");
        configuration.set("dfs.client.use.datanode.hostname", "true");

        FileSystem fileSystem = FileSystem.get(configuration);
        FSDataInputStream fis = fileSystem.open(new Path(src));

        FileOutputStream fos = new FileOutputStream(new File(filename));

        IOUtils.copy(fis, fos);
        IOUtils.closeQuietly(fis);
        IOUtils.closeQuietly(fos);
        fileSystem.close();
    }

    public static void mergeFile(String localDir, String dest) throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://knot1:8020");
        configuration.set("dfs.client.use.datanode.hostname", "true");

        FileSystem fileSystem = FileSystem.get(configuration);
        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path(dest));

        LocalFileSystem localFileSystem = FileSystem.getLocal(new Configuration());

        FileStatus[] fileStatuses = localFileSystem.listStatus(new Path(localDir));
        for(FileStatus fileStatus : fileStatuses) {
            Path path = fileStatus.getPath();
            FSDataInputStream fsDataInputStream = localFileSystem.open(path);
            IOUtils.copy(fsDataInputStream, fsDataOutputStream);
            IOUtils.closeQuietly(fsDataInputStream);
        }
        IOUtils.closeQuietly(fsDataOutputStream);
        localFileSystem.close();
        fileSystem.close();
    }
}

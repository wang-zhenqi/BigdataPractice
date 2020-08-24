package priv.zhenqi.mr.inputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * 自定义RecordReader类，用于读取文件的各个切片，这个应用里由于不需要切片，于是就相当于读取各文件。
 */
public class MyRecordReader extends RecordReader<NullWritable, BytesWritable> {
    private FileSplit fileSplit;
    private Configuration configuration;
    private BytesWritable bytesWritable;
    private boolean flag = false;

    /**
     * 初始化RecordReader的方法，只在最初时调用一次，只要拿到了文件的切片就拿到了文件的内容。
     *
     * @param inputSplit         输入文件的切片
     * @param taskAttemptContext 上下文
     */
    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) {
        this.fileSplit = (FileSplit) inputSplit;
        this.configuration = taskAttemptContext.getConfiguration();
        bytesWritable = new BytesWritable();
    }

    /**
     * 读取数据
     * 如果返回true，表示文件已经读取完成，不用再继续往下读了；否则就还未完成，继续读下一行
     *
     * @return boolean类型返回值，指示文件是否读取完成。
     * @throws IOException 抛出异常
     */
    @Override
    public boolean nextKeyValue() throws IOException {
        if(!flag) {
            long length = fileSplit.getLength();
            byte[] bytes = new byte[(int) length];

            Path path = fileSplit.getPath();
            FileSystem fileSystem = path.getFileSystem(configuration);
            FSDataInputStream open = fileSystem.open(path);

            IOUtils.readFully(open, bytes, 0, (int) length);
            bytesWritable.set(bytes, 0, (int) length);
            flag = true;
            return true;
        }
        return false;
    }

    /**
     * 获取数据的key_in
     *
     * @return 由于之前定义的key_in的类型是NullWritable，所以这里返回一个NullWritable对象。
     */
    @Override
    public NullWritable getCurrentKey() {
        return NullWritable.get();
    }

    /**
     * 获取数据的value_in
     *
     * @return 定义了value_in的类型是BytesWritable，就将nextKeyValue()中得到的value返回。
     */
    @Override
    public BytesWritable getCurrentValue() {
        return bytesWritable;
    }

    /**
     * 读取文件的进度，在这里没有太大作用，未读取完进度就是0，读取完了就是1.
     *
     * @return 0或1
     */
    @Override
    public float getProgress() {
        return flag ? 1.0f : 0.0f;
    }

    /**
     * 关闭资源，在此应用中不需做什么操作。
     */
    @Override
    public void close() {

    }
}

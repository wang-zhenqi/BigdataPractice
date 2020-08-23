package priv.zhenqi.mr.inputformat;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * 此自定义InputFormat类的作用是以文件为单位（不需要切分）读取多个小文件。
 */
public class MyInputFormat extends FileInputFormat<NullWritable, BytesWritable> {
    /**
     * 创建一个RecordReader用来读取每个分片，这里需要自定义RecordReader类：
     * 见{@link MyRecordReader}
     *
     * @param inputSplit         被切分出来的逻辑分片，之后每个inputSplit都对应一个Mapper
     * @param taskAttemptContext 上下文环境
     * @return 返回RecordReader
     */
    @Override
    public RecordReader<NullWritable, BytesWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) {
        MyRecordReader myRecordReader = new MyRecordReader();
        myRecordReader.initialize(inputSplit, taskAttemptContext);
        return myRecordReader;
    }

    /**
     * 这个方法决定了读入的文件是否可以切分，按照此应用的需求，文件不需要切分，因此直接返回false。
     *
     * @param context  上下文
     * @param filename 文件名
     * @return 返回值指示是否可以切分
     */
    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }
}

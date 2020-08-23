package priv.zhenqi.mr.inputformat;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * 此Mapper类接受自定义InputFormat的输入数据，key_in为null, value_in为各文件的全部内容
 */
public class MyInputFormatMapper extends Mapper<NullWritable, BytesWritable, Text, BytesWritable> {
    /**
     * map逻辑的实现。这里的输出是以各文件名为key_out，以对应文件内容为value_out。
     *
     * @param key     key_in，null
     * @param value   value_in，文件内容
     * @param context 上下文环境
     * @throws IOException          抛出异常
     * @throws InterruptedException 抛出异常
     */
    @Override
    protected void map(NullWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
        FileSplit inputSplit = (FileSplit) context.getInputSplit();
        //获取文件名称
        String name = inputSplit.getPath().getName();
        context.write(new Text(name), value);
    }
}

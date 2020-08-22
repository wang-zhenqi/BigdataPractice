package priv.zhenqi.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 自定义mapper类需要继承Mapper父类，有四个泛型
 * key_in: LongWritable(标准类型long的封装)，表示行偏移量
 * value_in: Text(String的封装)，表示每行的内容
 * key_out: Text，每个单词
 * value_out: IntWritable(标准类型int的封装)，值为1，表示key_out代表的单词出现了一次
 */
public class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    /**
     * 需要覆写map()方法，每读取一行数据都要调用一次
     * 例如：(key_in, value_in) = (1, hello,world)
     * (key_out, value_out) = (hello, 1)
     * (key_out, value_out) = (world, 1)
     *
     * @param key     对应key_in
     * @param value   对应value_in
     * @param context 上下文对象，承上启下，不需要进行特别的操作，但是需要用它将数据传递下去
     * @throws IOException          抛出异常
     * @throws InterruptedException 抛出异常
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] splitWords = value.toString().split(" ");
        Text text = new Text();
        IntWritable appearance = new IntWritable(1);
        for(String word : splitWords) {
            text.set(word);
            context.write(text, appearance);
        }
    }
}

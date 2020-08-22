package priv.zhenqi.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 自定义的reducer类需继承Reducer父类，同样有四个泛型
 * key_in: Text，每个单词
 * value_in: IntWritable，经过第六步分组后的该单词数目
 * key_out: Text，每个单词
 * value_out: IntWritable，该单词的总数
 */
public class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    /**
     * 此时相同的单词将会被发送至同一个reducer，这样就需要把每个单词的数量加起来即可得到总数
     *
     * @param key     对应key_in
     * @param values  对应value_in
     * @param context 上下文对象
     * @throws IOException          抛出异常
     * @throws InterruptedException 抛出异常
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for(IntWritable value : values) {
            count += value.get();
        }
        IntWritable total = new IntWritable(count);
        context.write(key, total);
    }
}

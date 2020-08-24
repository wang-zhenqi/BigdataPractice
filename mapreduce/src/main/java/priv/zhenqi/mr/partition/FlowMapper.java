package priv.zhenqi.mr.partition;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 自定义的Mapper类，用于读取一行流量记录，并且提取所需信息，包装到新的键值对中发送出去。
 */
public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
    Text key_out;
    FlowBean value_out;

    /**
     * 设置成员变量，这个方法会在每个MapTask开始时执行一次
     *
     * @param context 上下文
     */
    @Override
    protected void setup(Context context) {
        key_out = new Text();
        value_out = new FlowBean();
    }

    /**
     * map逻辑。将读入的流量记录拆分，电话号码字段作为key_out，流量信息包装在FlowBean中作为value_out。
     *
     * @param key     key_in，代表记录文件中的行号，在这里没有用到。
     * @param value   value_in，每一行的文件内容。
     * @param context 上下文。
     * @throws IOException          抛出异常
     * @throws InterruptedException 抛出异常
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        key_out.set(fields[1]);
        value_out.setUpCountFlow(Integer.parseInt(fields[6]));
        value_out.setDownCountFlow(Integer.parseInt(fields[7]));
        value_out.setUpFlow(Integer.parseInt(fields[8]));
        value_out.setDownFlow(Integer.parseInt(fields[9]));
        context.write(key_out, value_out);
    }
}

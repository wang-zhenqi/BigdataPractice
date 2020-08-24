package priv.zhenqi.mr.partition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 自定义的Reducer类用于读取Partitioner输出的结果。输入的key是手机号，输入的value是该手机号的
 * 所有流量记录。对这些记录进行累加就得到了该手机号的流量总量。最后将这结果发送出去。
 */
public class FlowReducer extends Reducer<Text, FlowBean, Text, Text> {
    private FlowBean totalFlow;

    /**
     * 在ReduceTask开始时调用
     *
     * @param context 上下文
     */
    @Override
    protected void setup(Context context) {
        totalFlow = new FlowBean();
    }

    /**
     * reduce逻辑。统计每个手机号的流量总量。
     *
     * @param key     key_in，手机号
     * @param values  value_in，流量记录的集合
     * @param context 上下文
     * @throws IOException          抛出异常
     * @throws InterruptedException 抛出异常
     */
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        int totalUpCountFlow = 0;
        int totalDownCountFlow = 0;
        int totalUpFlow = 0;
        int totalDownFlow = 0;
        for(FlowBean value : values) {
            totalUpCountFlow += value.getUpCountFlow();
            totalDownCountFlow += value.getDownCountFlow();
            totalUpFlow += value.getUpFlow();
            totalDownFlow += value.getDownFlow();
        }
        totalFlow.setUpCountFlow(totalUpCountFlow);
        totalFlow.setDownCountFlow(totalDownCountFlow);
        totalFlow.setUpFlow(totalUpFlow);
        totalFlow.setDownFlow(totalDownFlow);
        context.write(key, new Text(totalFlow.toString()));
    }
}

package priv.zhenqi.mr.sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 此Mapper类很简单，将源文件中的所需字段提取出来装入FlowSortBean中即可。
 */
public class FlowSortMapper extends Mapper<LongWritable, Text, FlowSortBean, NullWritable> {
    private FlowSortBean flowSortBean;

    @Override
    protected void setup(Context context) {
        flowSortBean = new FlowSortBean();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        flowSortBean.setPhoneNum(fields[1]);
        flowSortBean.setUpCountFlow(Long.parseLong(fields[6]));
        flowSortBean.setDownCountFlow(Long.parseLong(fields[7]));
        flowSortBean.setUpFlow(Long.parseLong(fields[8]));
        flowSortBean.setDownFlow(Long.parseLong(fields[9]));
        context.write(flowSortBean, NullWritable.get());
    }
}

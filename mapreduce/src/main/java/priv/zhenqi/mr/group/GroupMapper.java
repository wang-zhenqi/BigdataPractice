package priv.zhenqi.mr.group;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 此Mapper类的逻辑很简单，将读入的数据包装到OrderBean中，只需要orderId和price字段即可。
 */
public class GroupMapper extends Mapper<LongWritable, Text, OrderBean, Text> {
    private OrderBean orders;

    @Override
    protected void setup(Context context) {
        orders = new OrderBean();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");

        orders.setOrderId(fields[0]);
        orders.setPrice(Double.parseDouble(fields[2]));

        String value_out = fields[1] + "\t" + fields[2];
        context.write(orders, new Text("[" + value_out + "]"));
    }
}

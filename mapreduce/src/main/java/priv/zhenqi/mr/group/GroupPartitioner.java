package priv.zhenqi.mr.group;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 根据OrderBean中的orderId来进行分区。
 */
public class GroupPartitioner extends Partitioner<OrderBean, Text> {
    @Override
    public int getPartition(OrderBean orderBean, Text text, int i) {
        return orderBean.getOrderId().hashCode() % i;
    }
}

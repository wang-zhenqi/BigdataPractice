package priv.zhenqi.mr.group;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 分组类，将同一订单（即orderId相同）的记录分在同一组。
 */
public class MyGroup extends WritableComparator {
    public MyGroup() {
        super(OrderBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean a1 = (OrderBean) a;
        OrderBean b1 = (OrderBean) b;
        return a1.getOrderId().compareTo(b1.getOrderId());
    }
}

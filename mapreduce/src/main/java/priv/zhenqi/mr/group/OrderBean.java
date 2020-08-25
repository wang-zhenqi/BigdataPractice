package priv.zhenqi.mr.group;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 这个JavaBean类是用于包装订单数据的各字段的。由于需要将各订单记录二次排序，因此需要将每一条记录都
 * 包装起来，作为Mapper的key_out，也会在之后的分区、排序、分组的过程中使用到此类。
 *
 */
public class OrderBean implements WritableComparable<OrderBean> {
    private String orderId;
    private Double price;

    /**
     * 比较规则：订单号升序，价格降序
     *
     * @param o 待比较的对象
     * @return 比较结果。-1：this小于对象o，0：this等于对象o，1：this大于对象o。
     */
    @Override
    public int compareTo(OrderBean o) {
        int orderIdCompare = orderId.compareTo(o.orderId);
        if(orderIdCompare == 0) {
            return -price.compareTo(o.price);
        } else {
            return orderIdCompare;
        }
    }

    /**
     * 序列化
     *
     * @param out 输出数据
     * @throws IOException 抛出异常
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(orderId);
        out.writeDouble(price);
    }

    /**
     * 反序列化
     *
     * @param in 输入数据
     * @throws IOException 抛出异常
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        orderId = in.readUTF();
        price = in.readDouble();
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    @Override
    public String toString() {
        return "OrderBean {orderId = " + orderId +
                ", price = " + price +
                "}";
    }
}

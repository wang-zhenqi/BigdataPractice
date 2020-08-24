package priv.zhenqi.mr.sort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 定义一个JavaBean来处理数据，这个JavaBean要作为Mapper的key_out，因此要实现
 * {@link WritableComparable}接口。
 * 这个类的作用是包装原始数据的若干字段。
 */
public class FlowSortBean implements WritableComparable<FlowSortBean> {
    private String phoneNum;
    private Long upCountFlow;
    private Long downCountFlow;
    private Long upFlow;
    private Long downFlow;

    /**
     * 将数据按照“下行包总数升序”且“上行总流量”降序的顺序输出。
     *
     * @param o 待比较的对象
     * @return -1：this小于参数o，0：this等于参数o，1：this大于参数o。
     */
    @Override
    public int compareTo(FlowSortBean o) {
        int i = downCountFlow.compareTo(o.downCountFlow);
        if(i == 0) {
            i = -upFlow.compareTo(o.upFlow);
        }
        return i;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(phoneNum);
        out.writeLong(upCountFlow);
        out.writeLong(downCountFlow);
        out.writeLong(upFlow);
        out.writeLong(downFlow);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        phoneNum = in.readUTF();
        upCountFlow = in.readLong();
        downCountFlow = in.readLong();
        upFlow = in.readLong();
        downFlow = in.readLong();
    }

    public String getPhoneNum() {
        return phoneNum;
    }

    public void setPhoneNum(String phoneNum) {
        this.phoneNum = phoneNum;
    }

    public Long getUpCountFlow() {
        return upCountFlow;
    }

    public void setUpCountFlow(Long upCountFlow) {
        this.upCountFlow = upCountFlow;
    }

    public Long getDownCountFlow() {
        return downCountFlow;
    }

    public void setDownCountFlow(Long downCountFlow) {
        this.downCountFlow = downCountFlow;
    }

    public Long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(Long upFlow) {
        this.upFlow = upFlow;
    }

    public Long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(Long downFlow) {
        this.downFlow = downFlow;
    }

    @Override
    public String toString() {
        return "FlowSortBean {phoneNum = " + phoneNum +
                ", upCountFlow = " + upCountFlow +
                ", downCountFlow = " + downCountFlow +
                ", upFlow = " + upFlow +
                ", downFlow = " + downFlow +
                "}";
    }
}

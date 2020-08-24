package priv.zhenqi.mr.partition;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * FlowBean类是Mapper的value_out的类型，也是Paritioner的输入value。于是需要实现
 * {@link Writable}接口来实现数据的序列化与反序列化。
 * 注意：当使用JavaBean作为value数据时，实现Writable接口即可，但是如果作为key数据，则需要实现
 * {@link org.apache.hadoop.io.WritableComparable}接口，这是由于key涉及到MapReduce
 * 中排序的问题。
 */
public class FlowBean implements Writable {
    private Integer upCountFlow = 0;
    private Integer downCountFlow = 0;
    private Integer upFlow = 0;
    private Integer downFlow = 0;

    /**
     * 序列化方法
     *
     * @param out 待序列化的数据
     * @throws IOException 抛出异常
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(upCountFlow);
        out.writeInt(downCountFlow);
        out.writeInt(upFlow);
        out.writeInt(downFlow);
    }

    /**
     * 反序列化方法
     *
     * @param in 待反序列化的数据
     * @throws IOException 抛出异常
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        upCountFlow = in.readInt();
        downCountFlow = in.readInt();
        upFlow = in.readInt();
        downFlow = in.readInt();
    }

    public Integer getUpCountFlow() {
        return upCountFlow;
    }

    public void setUpCountFlow(Integer upCountFlow) {
        this.upCountFlow = upCountFlow;
    }

    public Integer getDownCountFlow() {
        return downCountFlow;
    }

    public void setDownCountFlow(Integer downCountFlow) {
        this.downCountFlow = downCountFlow;
    }

    public Integer getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(Integer upFlow) {
        this.upFlow = upFlow;
    }

    public Integer getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(Integer downFlow) {
        this.downFlow = downFlow;
    }

    @Override
    public String toString() {
        return "FlowBean {" +
                "upCountFlow = " + upCountFlow +
                ", downCountFlow = " + downCountFlow +
                ", upFlow = " + upFlow +
                ", downFlow = " + downFlow +
                "}";
    }
}

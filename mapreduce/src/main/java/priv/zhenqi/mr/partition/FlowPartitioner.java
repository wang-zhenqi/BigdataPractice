package priv.zhenqi.mr.partition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class FlowPartitioner extends Partitioner<Text, FlowBean> {
    private static final int PARTITION_NUM = 6;
    @Override
    public int getPartition(Text text, FlowBean flowBean, int i) {
        String phoneNum = text.toString();
        if(phoneNum != null && !phoneNum.equals("")) {
            if(phoneNum.startsWith("135")) {
                return 0;
            } else if(phoneNum.startsWith("136")) {
                return 1;
            } else if(phoneNum.startsWith("137")) {
                return 2;
            } else if(phoneNum.startsWith("138")) {
                return 3;
            } else if(phoneNum.startsWith("139")) {
                return 4;
            } else {
                return 5;
            }
        } else {
            return 5;
        }
    }

    public static int getNumOfPartition() {
        return PARTITION_NUM;
    }
}

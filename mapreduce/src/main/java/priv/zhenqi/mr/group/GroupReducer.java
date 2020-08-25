package priv.zhenqi.mr.group;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * 数据到达Reducer是就已经是有序的了，只需要输出前N个记录即可。
 */
public class GroupReducer extends Reducer<OrderBean, Text, Text, Text> {
    @Override
    protected void reduce(OrderBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int topN = Integer.parseInt(context.getConfiguration().get("topN"));
        Text result = new Text();
        StringBuilder sb = new StringBuilder();
        sb.append('{');
        Iterator<Text> it = values.iterator();
        while(it.hasNext()) {
            if(topN > 0) {
                sb.append(it.next().toString());
            } else {
                break;
            }
            if(--topN > 0 && it.hasNext()) {
                sb.append(", ");
            }
        }
        sb.append("}");
        result.set(sb.toString());
        context.write(new Text(key.getOrderId()), result);
    }
}

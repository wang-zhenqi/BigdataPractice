package priv.zhenqi.mr.group;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 此程序的作用是读取订单记录，将每笔订单中的价格最高的N项统计出来。
 * 这里的参数N可通过命令行传入，否则的话默认值为1.
 * 订单记录的样本数据如下：
 * 订单号            产品编号  价格
 * Order_0000001	Pdt_01	222.8
 *
 * 这就涉及到二次排序，除了要按订单号排序，还需要对价格也进行排序，于是就引入{@link OrderBean}类将
 * 订单号与价格包装在一起。以OrderBean作为Mapper的key_out。
 *
 * 但是根据需求，同一笔订单的记录应该被分在同一个分区，乃至同一个分组，因此在分区和分组的操作中不能用
 * OrderBean作为依据，而是要用订单号。于是还要定义{@link GroupPartitioner}和{@link MyGroup}
 * 两个类。
 *
 * 在输出时，由{@link GroupReducer}负责输出前N个记录。
 */
public class GroupMain extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        if(args.length == 3) {
            configuration.set("topN", args[2]);
        } else {
            configuration.set("topN", "1");
        }
        int run = ToolRunner.run(configuration, new GroupMain(), args);
        System.exit(run);
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(super.getConf(), "group");
        job.setJarByClass(GroupMain.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path(args[0]));

        job.setMapperClass(GroupMapper.class);
        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(Text.class);

        job.setPartitionerClass(GroupPartitioner.class);

        job.setGroupingComparatorClass(MyGroup.class);

        job.setReducerClass(GroupReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setNumReduceTasks(3);

        return job.waitForCompletion(true) ? 0 : -1;
    }
}

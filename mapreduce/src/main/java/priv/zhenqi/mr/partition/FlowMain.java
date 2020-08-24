package priv.zhenqi.mr.partition;

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
 * 此程序的目的是从手机用户的流量记录表中，统计出各手机号的流量使用情况，并且按照手机号的前缀分文件输出。
 * 由于本地无法用多线程模拟本地程序运行了，因此要将此程序打包上传至集群运行。
 * <p>
 * 流量记录文件的样本如下：
 * 1363157985066	13726230503	00-FD-07-A4-72-B8:CMCC	120.196.100.82	i02.c.aliimg.com	游戏娱乐	24	27	2481	24681	200
 * 一共分为11个字段，格式说明如下：
 * 序号    字段             类型         描述
 * 1      phoneNumber     String      手机号码
 * 6      upCountFlow     long        上行包数
 * 7      downCountFlow   long        下行包数
 * 8      upFlow          long        上行数据量
 * 9      downFlow        long        下行数据量
 */
public class FlowMain extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        //configuration.set("mapreduce.framework.name","local");
        //configuration.set("yarn.resourcemanager.hostname","local");

        int run = ToolRunner.run(configuration, new FlowMain(), args);
        System.exit(run);
    }

    /**
     * 组装MapReduce程序逻辑，设置各项参数，运行程序。
     *
     * @param args 传入的参数，包含了待读取及待输出的文件路径。
     * @return 指示运行是否成功，0：成功，-1：失败。
     * @throws Exception 抛出异常
     */
    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(super.getConf(), "flow_summation");
        //如果程序打包运行必须要设置这一句
        job.setJarByClass(FlowMain.class);

        //这里的InputFormat类不要设置成接口类了，之前记成了FileInputFormat，结果就导致了
        //无法实例化的异常。
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path(args[0]));

        job.setMapperClass(FlowMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        job.setPartitionerClass(FlowPartitioner.class);
        job.setNumReduceTasks(FlowPartitioner.getNumOfPartition());

        job.setReducerClass(FlowReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        return (job.waitForCompletion(true) ? 0 : -1);
    }
}

package priv.zhenqi.mr.wordcount;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * WordCount程序的入口类，包含main()方法
 */
public class WordCount extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        //提交run方法之后，得到一个程序的退出状态码，退出整个进程
        int run = ToolRunner.run(null, new WordCount(), args);
        System.exit(run);
    }

    /**
     * 需要实现Tool接口中的run()方法，用于将MR程序的八个步骤组装起来。
     *
     * @param args 传入的参数，这里可以是待处理的源文件路径以及保存结果的目标文件路径
     * @return 0表示成功，-1表示失败
     * @throws Exception 抛出异常
     */
    @Override
    public int run(String[] args) throws Exception {
        //获取Job对象，组装八个步骤
        Job job = Job.getInstance();
        job.setJobName("mr_wordcount_demo");

        //实际工作当中，程序运行完成之后一般都是打包到集群上面去运行，打成一个jar包
        //如果要打包到集群上面去运行，必须添加一下设置
        job.setJarByClass(WordCount.class);

        //第一步：指定InputFormat，读取文件，解析成键值对
        job.setInputFormatClass(TextInputFormat.class);
        //指定源文件的路径
        TextInputFormat.addInputPath(job, new Path(args[0]));

        //第二步：自定义map逻辑，将上一步输入的键值对转换为新的键值对
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //第三步分区和第四步排序可省略
        //第五步组合适用于单词计数的程序，使用它可以减少MapTask落盘以及向ReduceTask传输的数据量
        //Combiner的本质也是reduce，需要继承Reducer类，在这个程序中，可以直接使用MyReducer即可
        job.setCombinerClass(MyReducer.class);

        //第七步：自定义reduce逻辑
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //第八步：输出最终的键值对进行保存，指定输出目标文件路径
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        //提交job任务
        return job.waitForCompletion(true) ? 0 : -1;
    }
}

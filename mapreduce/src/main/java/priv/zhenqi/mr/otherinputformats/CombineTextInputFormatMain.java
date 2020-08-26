package priv.zhenqi.mr.otherinputformats;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * 整个类作为{@link CombineTextInputFormat}的演示类，包含了main()方法、run()方法，以及Mapper和
 * Reducer的定义。仍然完成{@link priv.zhenqi.mr.wordcount.WordCount}的功能.
 */
public class CombineTextInputFormatMain extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();

        //提交run方法之后，得到一个程序的退出状态码
        int run = ToolRunner.run(configuration, new CombineTextInputFormatMain(), args);
        //根据我们 程序的退出状态码，退出整个进程
        System.exit(run);
    }

    /**
     * 实现Tool接口之后，需要实现一个run方法，
     * 这个run方法用于组装我们的程序的逻辑，其实就是组装八个步骤
     *
     * @param args 文件路径
     * @return 返回运行结果，0：成功，-1：失败。
     * @throws Exception 抛出异常
     */
    @Override
    public int run(String[] args) throws Exception {
        //获取Job对象，组装我们的八个步骤，每一个步骤都是一个class类
        Configuration conf = super.getConf();
        Job job = Job.getInstance(conf, CombineTextInputFormatMain.class.getSimpleName());

        //判断输出路径，是否存在，如果存在，则删除
        FileSystem fileSystem = FileSystem.get(conf);
        if(fileSystem.exists(new Path(args[1]))) {
            fileSystem.delete(new Path(args[1]), true);
        }

        //实际工作当中，程序运行完成之后一般都是打包到集群上面去运行，打成一个jar包
        //如果要打包到集群上面去运行，必须添加以下设置
        job.setJarByClass(CombineTextInputFormatMain.class);

        //第一步：读取文件，解析成key,value对，k1:行偏移量  v1：一行文本内容
        job.setInputFormatClass(CombineTextInputFormat.class);
        //虚拟存储切片最大值设置4m
        CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);//4M
        //指定我们去哪一个路径读取文件
        CombineTextInputFormat.addInputPath(job, new Path(args[0]));

        //第二步：自定义map逻辑，接受k1   v1  转换成为新的k2   v2输出
        job.setMapperClass(MyMapper.class);
        //设置map阶段输出的key,value的类型，其实就是k2  v2的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //第三步到六步：分区，排序，规约，分组都省略

        //第七步：自定义reduce逻辑
        job.setReducerClass(MyReducer.class);
        //设置key3  value3的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //第八步：输出k3  v3 进行保存
        job.setOutputFormatClass(TextOutputFormat.class);
        //一定要注意，输出路径是需要不存在的，如果存在就报错
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setNumReduceTasks(3);

        //提交job任务
        return job.waitForCompletion(true) ? 0 : -1;
    }

    private static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        IntWritable intWritable = new IntWritable(1);
        Text text = new Text();

        /**
         * 继承mapper之后，覆写map方法，每次读取一行数据，都会来调用一下map方法
         *
         * @param key：对应key_in
         * @param value:对应value_in
         * @param context          上下文对象。承上启下，承接上面步骤发过来的数据，通过context将数据发送到下面的步骤里面去
         * @throws IOException          抛出异常
         * @throws InterruptedException 抛出异常
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //获取我们的一行数据
            String line = value.toString();
            String[] split = line.split(",");

            for(String word : split) {
                //将每个单词出现都记做1次
                //key2  Text类型
                //v2  IntWritable类型
                text.set(word);
                //将我们的key2  v2写出去到下游
                context.write(text, intWritable);
            }
        }
    }

    private static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int result = 0;
            for(IntWritable value : values) {
                //将我们的结果进行累加
                result += value.get();
            }
            //继续输出我们的数据
            IntWritable intWritable = new IntWritable(result);
            //将我们的数据输出
            context.write(key, intWritable);
        }
    }
}
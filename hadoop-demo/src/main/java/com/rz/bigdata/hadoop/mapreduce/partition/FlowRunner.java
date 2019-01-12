package com.rz.bigdata.hadoop.mapreduce.partition;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by as on 2018/1/20.
 * 这个job实现如下知识点：
 * <p>
 *    根据自定义分组进行分组分发
 */
public class FlowRunner extends Configured implements Tool {

    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //设置这个job所用使用的jar路径在那个类的路径下
        job.setJarByClass(FlowRunner.class);
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);
        //Reduce输出
        job.setOutputKeyClass(Flow.class);
        job.setOutputValueClass(NullWritable.class);
        //设置map输出
        job.setMapOutputKeyClass(Flow.class);
        job.setMapOutputValueClass(NullWritable.class);
        //设定自定义分组
        job.setPartitionerClass(AreaPartition.class);
        //设置reduce的task数目
        job.setNumReduceTasks(3);
        //指定输入路径 这里会存储路径下所有文件
        FileInputFormat.setInputPaths(job, new Path("/partition/data/"));
        //指定输出路径
        FileOutputFormat.setOutputPath(job, new Path("/partition/result/"));
        //boolean 表示是否显示处理进度
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int result = ToolRunner.run(new Configuration(), new FlowRunner(), args);
        System.exit(result);
    }
}

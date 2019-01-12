package com.rz.bigdata.hadoop.mapreduce.objserialize;

import com.rz.bigdata.hadoop.mapreduce.baseuse.WCMapper;
import com.rz.bigdata.hadoop.mapreduce.baseuse.WCReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by as on 2018/1/20.
 * 这个job实现如下知识点：
 * <p>
 * hadoop的MapReduce中的序列化
 * hadoop的MapReduce中自定义对象排序
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
        //指定输入路径 这里会存储路径下所有文件
        FileInputFormat.setInputPaths(job, new Path("/objserialize/data/"));
        //指定输出路径
        FileOutputFormat.setOutputPath(job, new Path("/objserialize/result/"));
        //boolean 表示是否显示处理进度
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int result = ToolRunner.run(new Configuration(), new FlowRunner(), args);
        System.exit(result);
    }
}

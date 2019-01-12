package com.rz.bigdata.hadoop.mapreduce.baseuse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by as on 2018/1/19.
 * 描述一个job
 */
public class WCRunner {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //设置这个job所用使用的jar路径在那个类的路径下
        job.setJarByClass(WCRunner.class);
        job.setMapperClass(WCMapper.class);
        job.setReducerClass(WCReducer.class);
        //Reduce输入输出
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        //设置map输入输出
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        //指定输入路径 这里会存储路径下所有文件
        FileInputFormat.setInputPaths(job, new Path("/wc/data/"));
        //指定输出路径
        FileOutputFormat.setOutputPath(job, new Path("/wc/result/"));
        //boolean 表示是否显示处理进度
        job.waitForCompletion(true);
    }
}

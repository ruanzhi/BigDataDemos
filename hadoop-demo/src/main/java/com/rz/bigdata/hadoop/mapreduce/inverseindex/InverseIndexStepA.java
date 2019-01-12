package com.rz.bigdata.hadoop.mapreduce.inverseindex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.net.URI;

/**
 * Created by as on 2018/1/21.
 * 倒排索引第一个job
 */
public class InverseIndexStepA {


    public static class StepAMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String lineContent = value.toString();
            String[] contents = StringUtils.split(lineContent, ' ');
            //获取切片信息
            FileSplit inputSplit = (FileSplit) context.getInputSplit();
            //获取文件名字
            String fileName = inputSplit.getPath().getName();
            for (String content : contents) {
                context.write(new Text(content + "-->" + fileName), new LongWritable(1));
            }
        }
    }

    public static class StepARecuder extends Reducer<Text, LongWritable, Text, LongWritable> {

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

            long count = 0;
            for (LongWritable value : values) {
                count += value.get();
            }
            context.write(key, new LongWritable(count));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //设置这个job所用使用的jar路径在那个类的路径下
        job.setJarByClass(InverseIndexStepA.class);
        job.setMapperClass(StepAMapper.class);
        job.setReducerClass(StepARecuder.class);
        //Reduce输入输出
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        //设置map输入输出
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        //指定输入路径 这里会存储路径下所有文件
        FileInputFormat.setInputPaths(job, new Path("/ii/data/"));

        //如果路径存在就删除
        Path path = new Path("/ii/result/");
        checkAndDeleteExistsPath(path, conf);
        //指定输出路径
        FileOutputFormat.setOutputPath(job, path);
        //boolean 表示是否显示处理进度
        job.waitForCompletion(true);
    }

    public static void checkAndDeleteExistsPath(Path path, Configuration conf) throws Exception {
        URI uri = new URI("hdfs://192.168.1.113:9000/");
        FileSystem fileSystem = FileSystem.get(uri, conf, "root");
        if (fileSystem.exists(path)) {
            fileSystem.delete(path, true);
        }
    }
}

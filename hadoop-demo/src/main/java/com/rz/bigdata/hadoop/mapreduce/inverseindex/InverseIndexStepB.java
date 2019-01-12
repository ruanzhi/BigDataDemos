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
public class InverseIndexStepB {


    public static class StepBMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String lineContent = value.toString();
            String[] contents = StringUtils.split(lineContent, '\t');

            String[] keys = contents[0].split("-->");
            context.write(new Text(keys[0]), new Text(keys[1] + "-->" + contents[1]));
        }
    }

    public static class StepBRecuder extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            StringBuffer valueSB = new StringBuffer();
            for (Text value : values) {
                valueSB.append(value.toString()).append(",");
            }
            context.write(key, new Text(valueSB.toString().substring(0, valueSB.toString().length() - 1)));

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //设置这个job所用使用的jar路径在那个类的路径下
        job.setJarByClass(InverseIndexStepB.class);
        job.setMapperClass(StepBMapper.class);
        job.setReducerClass(StepBRecuder.class);
        //Reduce输入输出
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //设置map输入输出
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        //指定输入路径 这里会存储路径下所有文件
        FileInputFormat.setInputPaths(job, new Path("/ii/result/"));

        //如果路径存在就删除
        Path path = new Path("/ii/result1/");
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

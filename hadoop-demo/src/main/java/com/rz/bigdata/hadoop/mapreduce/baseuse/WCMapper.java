package com.rz.bigdata.hadoop.mapreduce.baseuse;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

/**
 * Created by as on 2018/1/19.
 * 在mapreduce中数据输入输出都是以key-value方式来封装的
 * <p>
 * Mapper中四个泛型参数含义：
 * 前两个是指定mapper输入数据类型，第一个参数KEYIN代表输入数据的key类型，第二参数VALUEIN代表输入数据value类型
 * 后两个参数是指定mapper输出数据类型，第三个参数KEYOUT代表输出数据的key类型，第二个参数VALUEOUT代表输出数据value类型
 * <p>
 * 默认情况下，框架传递给mapper的输入数据，key是文本中一行的起始偏移量，value是这一行内容
 */
public class WCMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    //mapreduce每读一行数据调用一次此方法
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String lineContent = value.toString();
        String[] contents = StringUtils.split(lineContent, ' ');
        //遍历此行的内容，输出格式为kv   k:1
        for (String content : contents) {
            context.write(new Text(content), new LongWritable(1));
        }
    }
}

package com.rz.bigdata.hadoop.mapreduce.baseuse;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by as on 2018/1/19.
 * Reducer在所用Mapper执行完毕之后才调用
 */
public class WCReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    /**
     * 这里会得到所有的内容对应次数,每一个<key,values[]>都会调用一次reduce
     * 格式为
     * v:[1,2,3]  这个 v单词总的次数就是1+2+3=6
     *
     * @param key     对应 v这个单词
     * @param values  对应[1,2,3]  需要遍历values求和
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long count = 0;
        for (LongWritable value : values) {
            count += value.get();
        }
        context.write(key, new LongWritable(count));
    }
}

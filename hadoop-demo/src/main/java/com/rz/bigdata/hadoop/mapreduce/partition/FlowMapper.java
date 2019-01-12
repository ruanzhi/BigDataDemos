package com.rz.bigdata.hadoop.mapreduce.partition;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

/**
 * Created by as on 2018/1/20.
 */
public class FlowMapper extends Mapper<LongWritable, Text, Flow, NullWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String lineContent = value.toString();
        String[] contents = StringUtils.split(lineContent, ' ');
        String phnnwNB = contents[0];
        long d_flow = Long.parseLong(contents[1]);
        long up_flow = Long.parseLong(contents[2]);
        Flow flow = new Flow(phnnwNB, up_flow, d_flow);
        context.write(flow, NullWritable.get());
    }
}

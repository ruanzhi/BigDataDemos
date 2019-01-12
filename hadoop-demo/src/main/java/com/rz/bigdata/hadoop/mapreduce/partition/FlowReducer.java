package com.rz.bigdata.hadoop.mapreduce.partition;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by as on 2018/1/20.
 */
public class FlowReducer extends Reducer<Flow, NullWritable, Flow, NullWritable> {

    List<String> flows = new ArrayList<String>();

    @Override
    protected void reduce(Flow key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        System.out.println(key);
        if (!flows.contains(key.getPhoneNB())) {
            context.write(key, NullWritable.get());
            flows.add(key.getPhoneNB());
        }

    }
}

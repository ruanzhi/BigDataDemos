package com.rz.bigdata.hadoop.mapreduce.objserialize;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by as on 2018/1/20.
 */
public class FlowReducer extends Reducer<Flow, NullWritable, Flow, NullWritable> {

    List<Flow> flows = new ArrayList<Flow>();

    @Override
    protected void reduce(Flow key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

        if (!flows.contains(key)) {
            context.write(key, NullWritable.get());
            flows.add(key);
        }

    }
}

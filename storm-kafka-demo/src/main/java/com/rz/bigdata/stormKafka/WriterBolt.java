package com.rz.bigdata.stormKafka;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;

/**
 * Created by as on 2018/1/23.
 */
public class WriterBolt extends BaseBasicBolt {
    private static final long serialVersionUID = -6586283337287975719L;

    private FileWriter writer = null;

    public void prepare(Map stormConf, TopologyContext context) {
        try {
            writer = new FileWriter("c:\\Users\\as\\Desktop\\" + "wordcount" + UUID.randomUUID().toString());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String s = tuple.getString(0);
        try {
            writer.write(s);
            writer.write("\n");
            writer.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}

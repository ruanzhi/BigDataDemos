package com.rz.bigdata.storm;

import backtype.storm.task.OutputCollector;
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
 * Created by as on 2018/1/22.
 * Suffix：后缀
 * 此bolt是给发送过来的数据添加后缀
 */
public class SuffixBolt extends BaseBasicBolt {
    FileWriter fileWriter = null;


    //在bolt组件运行过程中只会被调用一次
    @Override
    public void prepare(Map stormConf, TopologyContext context) {

        try {
            fileWriter = new FileWriter("/home/ruanzhi/storm-0.9.2/stormoutput/" + UUID.randomUUID());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        //先拿到上一个组件发送过来的商品名称
        String upperGodName = tuple.getString(0);
        String doubleGodPrice = tuple.getString(1);
        String suffixGodName = upperGodName + "_itisok";

        //为上一个组件发送过来的商品名称添加后缀

        try {
            System.out.println(suffixGodName + "\t" + doubleGodPrice);
            fileWriter.write(suffixGodName + "\t" + doubleGodPrice);
            fileWriter.write("\n");
            fileWriter.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}

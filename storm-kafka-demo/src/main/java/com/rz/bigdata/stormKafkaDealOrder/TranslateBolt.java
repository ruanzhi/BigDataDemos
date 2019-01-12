package com.rz.bigdata.stormKafkaDealOrder;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.List;

/**
 * Created by as on 2018/1/23.
 */
public class TranslateBolt extends BaseBasicBolt {
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        List<Object> list = tuple.getValues();

        String id = (String) list.get(0);
        String memberid = (String) list.get(1);
        String totalprice = (String) list.get(2);
        String discount = (String) list.get(3);
        String sendpay = (String) list.get(4);
        if ("0".equals(sendpay)) {
            sendpay = "-1";
        }
        basicOutputCollector.emit(new Values(id, memberid, totalprice, discount, sendpay));
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("id", "memberid", "totalprice", "discount", "sendpay"));
    }
}

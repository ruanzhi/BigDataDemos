package com.rz.bigdata.storm;


import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

/**
 * Created by as on 2018/1/22.
 * 此bolt是给发送过来的数据添加转换为大写
 */
public class UpperBolt extends BaseBasicBolt {

    //处理业务逻辑
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
//先获取到上一个组件传递过来的数据,数据在tuple里面
        String godName = tuple.getString(0);
        String godPrice = tuple.getString(1);
        //将商品名转换成大写
        String godNameUpper = godName.toUpperCase();
        //价格双倍
        double doubleGodPrice = Double.parseDouble(godPrice) * 2;
        //将转换完成的商品名发送出去
        basicOutputCollector.emit(new Values(godNameUpper, doubleGodPrice + ""));
    }

    //声明该bolt组件要发出去的tuple的字段
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("upper_god_name", "double_god_price"));
    }
}

package com.rz.bigdata.storm;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Map;
import java.util.Random;

/**
 * Created by as on 2018/1/22.
 */
public class RandomWordSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    //模拟一些数据
    String[] words = {"iphone", "xiaomi", "mate", "sony", "sumsung", "moto", "meizu"};
    String[] prices = {"1234", "3211", "2222", "1232", "2345", "3333", "4444"};

    //初始化方法，在spout组件实例化时调用一次
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
    }

    //不断地往下一个组件发送tuple消息
    //这里面是该spout组件的核心逻辑
    public void nextTuple() {
//可以从kafka消息队列中拿到数据,简便起见，我们从words数组中随机挑选一个商品名发送出去
        Random random = new Random();
        int index = random.nextInt(words.length);

        //通过随机数拿到一个商品名
        String godName = words[index];
        String godPrice = prices[index];

        //将商品名封装成tuple，发送消息给下一个组件
        collector.emit(new Values(godName, godPrice));

        //每发送一个消息，休眠500ms
        Utils.sleep(500);

    }

    //声明本spout组件发送出去的tuple中的数据的字段名，这里需要和nextTuple中发送的tuple消息值对应
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        outputFieldsDeclarer.declare(new Fields("god_name", "god_price"));
    }
}

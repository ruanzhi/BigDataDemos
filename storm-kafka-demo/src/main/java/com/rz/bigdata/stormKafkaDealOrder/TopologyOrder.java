package com.rz.bigdata.stormKafkaDealOrder;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

/**
 * Created by as on 2018/1/23.
 * topology ：拓扑结构
 */
public class TopologyOrder {
    public static void main(String[] args) {
        String topic = "order";
        String zkRoot = "/kafka-storm-order";//消费者要往zookeeper里面写些信息，这里需要给一个zookeeper根目录
        String spoutId = "KafkaSpout";//spout需要一个名字
        BrokerHosts brokerHosts = new ZkHosts("192.168.1.112:2181");
        SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, topic, zkRoot, spoutId);
        spoutConfig.forceFromStart = true;//设置从哪里开始读，这个设置从开头开始读
        spoutConfig.scheme = new SchemeAsMultiScheme(new MessageScheme());
        TopologyBuilder builder = new TopologyBuilder();
        //设置一个spout用来从kaflka消息队列中读取数据并发送给下一级的bolt组件，此处用的spout组件并非自定义的，而是storm中已经开发好的KafkaSpout
        builder.setSpout("KafkaSpout", new KafkaSpout(spoutConfig));
        builder.setBolt("checkorder", new CheckOrderBolt()).shuffleGrouping(spoutId);
        builder.setBolt("translate", new TranslateBolt(), 4).shuffleGrouping("checkorder");
        builder.setBolt("saveOrderBolt", new SaveOrderBolt(), 4).shuffleGrouping("translate");
        Config conf = new Config();
        conf.setNumWorkers(4);
        conf.setNumAckers(0);
        conf.setDebug(false);

        //LocalCluster用来将topology提交到本地模拟器运行，方便开发调试
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("orderdeal", conf, builder.createTopology());
    }

}

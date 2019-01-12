package com.rz.bigdata.stormKafka;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

/**
 * Created by as on 2018/1/23.
 * kefka与storm整合，其实就是storm-kafka.jar这个jar里面提供一个spot
 * <p>
 * 开发者只需要给这个spot传递必要参数信息，其他的spot与kafka通信不需要我们亲自编写
 * KafkaSpot里面封装好了的
 */
public class KafkaTopo {

    public static void main(String[] args) {
        String topic = "rz";
        String zkRoot = "/kafka-storm";//消费者要往zookeeper里面写些信息，这里需要给一个zookeeper根目录
        String spoutId = "KafkaSpout";//spout需要一个名字
        BrokerHosts brokerHosts = new ZkHosts("192.168.1.112:2181");
        SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, topic, zkRoot, spoutId);
        spoutConfig.forceFromStart = true;
        spoutConfig.scheme = new SchemeAsMultiScheme(new MessageScheme());
        TopologyBuilder builder = new TopologyBuilder();
        //设置一个spout用来从kaflka消息队列中读取数据并发送给下一级的bolt组件，此处用的spout组件并非自定义的，而是storm中已经开发好的KafkaSpout
        builder.setSpout("KafkaSpout", new KafkaSpout(spoutConfig));
        //按字段分组，按数据中field值进行分组；field中对应的相同值的Tuple被发送到相同的Task
        builder.setBolt("word-spilter", new WordSpliter()).shuffleGrouping(spoutId);
        builder.setBolt("writer", new WriterBolt(), 4).fieldsGrouping("word-spilter", new Fields("word"));
        Config conf = new Config();
        conf.setNumWorkers(4);
        conf.setNumAckers(0);
        conf.setDebug(false);

        //LocalCluster用来将topology提交到本地模拟器运行，方便开发调试
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("WordCount", conf, builder.createTopology());
    }
}

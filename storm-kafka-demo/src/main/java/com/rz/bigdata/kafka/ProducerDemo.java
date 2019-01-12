package com.rz.bigdata.kafka;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;
import java.util.Random;

/**
 * Created by as on 2018/1/22.
 */
public class ProducerDemo {

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put("zk.connect", "192.168.1.112:2181");
        props.put("metadata.broker.list", "192.168.1.112:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        ProducerConfig config = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<String, String>(config);
        Random r = new Random();
        for (int i = 1; i <= 100; i++) {
            int id = r.nextInt(10000000);
            int memberid = r.nextInt(100000);
            int totalprice = r.nextInt(1000) + 100;
            int discount = r.nextInt(100);
            int sendpay = r.nextInt(3);
            StringBuffer data = new StringBuffer();
            data.append(String.valueOf(id))
                    .append("\t")
                    .append(String.valueOf(memberid))
                    .append("\t")
                    .append(String.valueOf(totalprice))
                    .append("\t")
                    .append(String.valueOf(discount))
                    .append("\t")
                    .append(String.valueOf(sendpay))
                    .append("\t")
                    .append("2018-01-23");
            System.out.println(data.toString());
            producer.send(new KeyedMessage<String, String>("order", data.toString()));
            Thread.sleep(500);
        }
        producer.close();
    }
}

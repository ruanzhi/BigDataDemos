package com.rz.bigdata.stormKafkaDealOrder;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.rz.bigdata.utils.DateUtils;
import org.apache.commons.lang.StringUtils;

/**
 * Created by as on 2018/1/23.
 * 订单号       用户id       原金额        优惠价     标示字段    下单时间
 *  id        memberid     totalprice    discount   sendpay     createdate
 * <p>
 * 检测订单有效性
 */
public class CheckOrderBolt extends BaseBasicBolt {
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String orderStr = tuple.getString(0);
        if (orderStr != null && orderStr.length() > 0) {
            String[] values = orderStr.split("\t");
            if (values.length == 6) {
                String id = values[0];
                String memberid = values[1];
                String totalprice = values[2];
                String youhui = values[3];
                String sendpay = values[4];
                String createdate = values[5];

                if (StringUtils.isNotEmpty(id) && StringUtils.isNotEmpty(memberid) && StringUtils.isNotEmpty(totalprice)) {
                    if (DateUtils.isDate(createdate, "2014-04-19")) {
                        basicOutputCollector.emit(new Values(id, memberid, totalprice, youhui, sendpay));
                    }
                }
            }
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("id", "memberid", "totalprice", "discount", "sendpay"));
    }
}

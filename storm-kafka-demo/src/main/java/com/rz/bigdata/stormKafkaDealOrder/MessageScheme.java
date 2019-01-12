package com.rz.bigdata.stormKafkaDealOrder;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * Created by as on 2018/1/23.
 * 这个相当于之前storm的spot组件，因为与kafka整合了
 * 这里相当于是把kafka发过来的二进制消息进行转码
 *
 */
public class MessageScheme implements Scheme {
    public List<Object> deserialize(byte[] bytes) {
        try {
            String msg = new String(bytes, "UTF-8");
            return new Values(msg);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return null;
    }

    public Fields getOutputFields() {
        return new Fields("msg");
    }
}

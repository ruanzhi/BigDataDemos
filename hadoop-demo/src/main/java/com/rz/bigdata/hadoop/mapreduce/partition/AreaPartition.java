package com.rz.bigdata.hadoop.mapreduce.partition;


import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by as on 2018/1/20.
 * <p>
 * 根据匹配规则返回最终数据落在第几组
 */
public class AreaPartition<Flow, VALUE> extends Partitioner<com.rz.bigdata.hadoop.mapreduce.partition.Flow, VALUE> {
    private static Map<String, Integer> areaMap = new HashMap<String, Integer>();

    static {
        areaMap.put("156", 0);
        areaMap.put("165", 1);
    }


    public int getPartition(com.rz.bigdata.hadoop.mapreduce.partition.Flow key, VALUE value, int numPartitions) {
        return areaMap.get(key.getPhoneNB().substring(0, 3)) == null ? 2 : areaMap.get(key.getPhoneNB().substring(0, 3));
    }
}

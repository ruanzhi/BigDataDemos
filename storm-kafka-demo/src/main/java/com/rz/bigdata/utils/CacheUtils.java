package com.rz.bigdata.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by as on 2018/1/23.
 * 这里可以使用redis memcache子类的nosql数据替代
 */
public class CacheUtils {

    private static List<String> list = new ArrayList<String>();

    public static boolean containkey(String key) {
        return list.contains(key);
    }

    public static void put(String key) {
        list.add(key);
    }
}

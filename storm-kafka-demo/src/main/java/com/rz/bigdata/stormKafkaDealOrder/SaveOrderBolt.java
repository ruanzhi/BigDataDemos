package com.rz.bigdata.stormKafkaDealOrder;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import com.netflix.curator.framework.recipes.locks.InterProcessMutex;
import com.rz.bigdata.utils.CacheUtils;
import com.rz.bigdata.utils.Constant;
import com.rz.bigdata.utils.JDBCUtil;
import com.rz.bigdata.utils.LockCuratorSrc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/**
 * Created by as on 2018/1/23.
 */
public class SaveOrderBolt extends BaseBasicBolt {
    private static Map<String, String> memberMap = null; //sendpay,counterMember
    private static List<String> cacheList = null;
    private Lock lock = null;//防止memberMap的数据丢失

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        memberMap = new HashMap<String, String>();
        cacheList = new ArrayList<String>();
        lock = new ReentrantLock();
        Timer timer = new Timer();
        timer.schedule(new SaveOrderBolt.cacheTimer(), new Date(), 5000);//定时处理数据
    }

    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        List<Object> list = tuple.getValues();

        String id = (String) list.get(0);
        String memberid = (String) list.get(1);
        String totalprice = (String) list.get(2);
        String discount = (String) list.get(3);
        String sendpay = (String) list.get(4);
        saveCounterMember(memberid, sendpay, totalprice, discount);//记录独立用户数
    }

    private void saveCounterMember(String memberid, String sendpay, String totalprice, String discount) {
        String key = sendpay + "_" + memberid;
        boolean ishasMem = CacheUtils.containkey(key);
        if (!ishasMem) {//没有的添加进去
            CacheUtils.put(key);
        }
        saveAndUpdateToMemberMap(sendpay, ishasMem, totalprice, discount);
    }

    /**
     * 这里就是拿出来对应支付方式的统计结果，进行更新
     *
     * @param sendpay
     * @param ishasMem
     * @param totalprice
     * @param discount
     */
    private void saveAndUpdateToMemberMap(String sendpay, boolean ishasMem, String totalprice, String discount) {
        if (memberMap == null) {
            memberMap = new HashMap<String, String>();
        }
        lock.lock();
        try {
            String order = memberMap.get(sendpay);//value = count(id),sum(totalPrice),sum(totalPrice - youhui),count(distinct memberid)
            if (order != null) {
                String[] vals = order.split(",");
                int id_num = Integer.valueOf(vals[0]) + 1;
                double tp = Double.valueOf(vals[1]) + Double.valueOf(totalprice);
                double etp = Double.valueOf(vals[2]) + (Double.valueOf(totalprice) - Double.valueOf(discount));
                int counter_member = Integer.valueOf(vals[3]) + (ishasMem ? 0 : 1);
                order = id_num + "," + tp + "," + etp + "," + counter_member;
            } else {
                order = 1 + "," + totalprice + "," + (Double.valueOf(totalprice) - Double.valueOf(discount)) + "," + (ishasMem ? 0 : 1);
            }
            System.out.println("sendpay = " + sendpay + "  value = " + order);
            memberMap.put(sendpay, order);
        } finally {
            lock.unlock();
        }
    }

    public class cacheTimer extends TimerTask {
        public void run() {
            Map<String, String> tmpMap = new HashMap<String, String>();
            lock.lock();
            try {
                tmpMap.putAll(memberMap);
                memberMap.clear();
            } finally {
                lock.unlock();
            }
            saveResultToMysql(tmpMap);
        }

    }

    private void saveResultToMysql(Map<String, String> tmpMap) {

        Connection conn = JDBCUtil.getConnectionByJDBC();
        //这里还需要加一个分布式锁，实现多实例之间插入数据时候，保证只有一个在操作数据库
        InterProcessMutex lock = new InterProcessMutex(LockCuratorSrc.getCF(), Constant.LOCKS_ORDER);
        try {
            while (lock.acquire(10, TimeUnit.MINUTES)) {
                for (Map.Entry<String, String> entry : tmpMap.entrySet()) {
                    //id,order_nums,p_total_price,y_total_price,order_members,sendpay
                    String key = entry.getKey();
                    String value = entry.getValue();
                    String[] vals = value.split(",");
                    int id_num = Integer.valueOf(vals[0]);
                    double tp = Double.valueOf(vals[1]);
                    double etp = Double.valueOf(vals[2]);
                    int counter_member = Integer.valueOf(vals[3]);
                    Statement stmt = conn.createStatement();
                    String sql = "select id,order_nums,p_total_price,y_total_price,order_members from total_order where sendpay='" + key + "'";
                    ResultSet set = stmt.executeQuery(sql);

                    int id = 0;
                    int order_nums = 0;
                    double p_total_price = 0;
                    double y_total_price = 0;
                    int order_member = 0;
                    while (set.next()) {
                        id = set.getInt(1);
                        order_nums = set.getInt(2);
                        p_total_price = set.getDouble(3);
                        y_total_price = set.getDouble(4);
                        order_member = set.getInt(5);
                    }
                    order_nums += id_num;
                    p_total_price += tp;
                    y_total_price += etp;
                    order_member += counter_member;
                    StringBuffer sBuffer = new StringBuffer();
                    if (id == 0) {// insert
                        sBuffer.append("insert into total_order(order_nums,p_total_price,y_total_price,order_members,sendpay) values(")
                                .append(order_nums + "," + p_total_price + "," + y_total_price + "," + order_member + ",'" + key + "')");
                    } else {// update
                        sBuffer.append("update total_order set order_nums=" + order_nums)
                                .append(",p_total_price=" + p_total_price)
                                .append(",y_total_price=" + y_total_price)
                                .append(",order_members=" + order_member)
                                .append(" where id=" + id);
                    }
                    System.out.println("sql = " + sBuffer.toString());
                    stmt = conn.createStatement();
                    stmt.executeUpdate(sBuffer.toString());
                    conn.commit();
                    stmt.close();
                }
                break;//跳出循环
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                }
                lock.release();
            } catch (Exception e) {
                e.printStackTrace();

            }
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

}

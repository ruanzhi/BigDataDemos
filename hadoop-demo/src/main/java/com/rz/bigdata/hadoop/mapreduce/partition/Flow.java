package com.rz.bigdata.hadoop.mapreduce.partition;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by as on 2018/1/20.
 * 流量bean
 */
public class Flow implements WritableComparable<Flow> {

    private String phoneNB;
    private long up_flow;
    private long d_flow;
    private long s_flow;

    //在反序列化时，反射机制需要调用空参构造函数，所以显示定义了一个空参构造函数
    public Flow() {
    }

    //为了对象数据的初始化方便，加入一个带参的构造函数
    public Flow(String phoneNB, long up_flow, long d_flow) {
        this.phoneNB = phoneNB;
        this.up_flow = up_flow;
        this.d_flow = d_flow;
        this.s_flow = up_flow + d_flow;
    }

    public String getPhoneNB() {
        return phoneNB;
    }

    public void setPhoneNB(String phoneNB) {
        this.phoneNB = phoneNB;
    }

    public long getUp_flow() {
        return up_flow;
    }

    public void setUp_flow(long up_flow) {
        this.up_flow = up_flow;
    }

    public long getD_flow() {
        return d_flow;
    }

    public void setD_flow(long d_flow) {
        this.d_flow = d_flow;
    }

    public long getS_flow() {
        return s_flow;
    }

    public void setS_flow(long s_flow) {
        this.s_flow = s_flow;
    }


    //将对象数据序列化到流中
    public void write(DataOutput out) throws IOException {

        out.writeUTF(phoneNB);
        out.writeLong(up_flow);
        out.writeLong(d_flow);
        out.writeLong(s_flow);

    }


    //从数据流中反序列出对象的数据
    //从数据流中读出对象字段时，必须跟序列化时的顺序保持一致

    public void readFields(DataInput in) throws IOException {
        phoneNB = in.readUTF();
        up_flow = in.readLong();
        d_flow = in.readLong();
        s_flow = in.readLong();

    }

    @Override
    public String toString() {

        return phoneNB + "\t" + up_flow + "\t" + d_flow + "\t" + s_flow;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Flow flow = (Flow) o;

        return phoneNB.equals(flow.phoneNB);

    }

    @Override
    public int hashCode() {
        return phoneNB.hashCode();
    }

    //谁大谁在前面
    public int compareTo(Flow o) {
        if (this.phoneNB.equals(o.getPhoneNB())) {
            this.up_flow += o.getUp_flow();
            this.d_flow += o.getD_flow();
            this.s_flow = this.up_flow - this.d_flow;
        }
        return s_flow > o.getS_flow() ? -1 : 1;
    }
}

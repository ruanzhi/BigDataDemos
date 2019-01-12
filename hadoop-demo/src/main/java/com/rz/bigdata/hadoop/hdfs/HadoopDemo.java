package com.rz.bigdata.hadoop.hdfs;


import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.fs.FileSystem;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by as on 2018/1/14.
 * <p>
 * 这里需要注意的点：
 * 1.当前连接hadoop 的hdfs时候需要注意用户角色问题，如果不指明角色，程序会自动默认采用你客户端程序中系统的角色
 * 所以这句代码需要注意：FileSystem.get(uri, conf, "root");   这里就是指明采用root角色
 * 2.如果访问hdfs报这个异常：Call From Rz/192.168.151.1 to 192.168.1.113:9000 failed on connection exception
 * 这时候你需要去hdfs服务器上面使用 netstat -an | grep 9000 命令
 * 查看是否有这个地址：192.168.1.113:9000
 * 如果没有这需要在 /etc/hosts文件下去掉里面 127.0.0.1 对应的自定义域名
 * 具体操作参照此博客：http://blog.csdn.net/renfengjun/article/details/25320043
 */

public class HadoopDemo {

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        try {
            URI uri = new URI("hdfs://192.168.1.113:9000/");
            FileSystem fileSystem = FileSystem.get(uri, conf, "root");
            Path f = new Path("hdfs://192.168.1.113:9000/rz/core-site.xml");
            FSDataInputStream in = fileSystem.open(f);
            FileOutputStream fos = new FileOutputStream(
                    "C:/Users/as/Desktop/core-site.xml");
            IOUtils.copy(in, fos);
            System.out.println("成功了");
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    FileSystem fileSystem;

    @Before
    public void init() {
        //读取classpath下面xxx-site.xml 配置文件,解析里面内容
        Configuration conf = new Configuration();
        //也可以代码设置,这里设置会覆盖配置文件中的信息
        //conf.set("fs.defaultFS","hdfs://192.168.1.113:9000");
        try {
            URI uri = new URI("hdfs://192.168.1.113:9000/");
            fileSystem = FileSystem.get(uri, conf, "root");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 上传
     */
    @Test
    public void upload() {

        try {
            if (fileSystem != null) {
                fileSystem.copyFromLocalFile(new Path(
                        "C:/Users/as/Desktop/core-site.xml"), new Path(
                        "hdfs://192.168.1.113:9000/rz/core-site.xml"));
            } else {
                System.out.println("上传失败");
            }

        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 下载
     */
    @Test
    public void download() {
        try {
            if (fileSystem == null) {
                System.out.println("下载失败");
                return;
            }

            fileSystem.copyToLocalFile(false, new Path(
                    "hdfs://192.168.1.113:9000/test.txt"), new Path(
                    "C:/Users/as/Desktop/hadoop.txt"), true);
            System.out.println("下载成功");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建文件夹
     */
    @Test
    public void mkdir() {

        try {
            fileSystem.mkdirs(new Path("/eclipsecreate/rz"));
            System.out.println("创建成功");
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void delete() {
        try {
            fileSystem.delete(new Path("/partition/result/"), true);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 查看hdfs上面的文件
     */
    @Test
    public void listFiles() {

        RemoteIterator<LocatedFileStatus> listFiles;
        try {
            // 第二参数表示是否递归遍历文件夹下面的文件 true代表可以
            listFiles = fileSystem.listFiles(new Path("/"), true);
            while (listFiles.hasNext()) {
                LocatedFileStatus next = listFiles.next();
                Path path = next.getPath();
                System.out.println(path.getName());//获取文件名字
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}

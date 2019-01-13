package com.flamingo.bigdata.hbase.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;

/**
 * @program: hbase-example
 * @Date: 2019/1/13 21:23
 * @Author: Qinhs
 * @Description: 数据库连接类
 */
public class HbaseConn {
    // 单例对象
    private static final HbaseConn INSTANCE = new HbaseConn();
    private static Configuration configuration;
    private static Connection connection;

    private HbaseConn(){
        try {
            if (null == configuration){
                configuration = HBaseConfiguration.create();
                configuration.set("hbase.zookeeper.quorum","120.79.236.88:2181");
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    private Connection getConnection(){
        if (null == connection || connection.isClosed()){
            try {
                connection = ConnectionFactory.createConnection(configuration);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        return connection;
    }

    public static Connection getHbaseConn(){
        return INSTANCE.getConnection();
    }

    public static Table getTable(String tableName) throws IOException {
        return INSTANCE.getConnection().getTable(TableName.valueOf(tableName));
    }

    public static void closeConn(){
        if (null != connection){
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}

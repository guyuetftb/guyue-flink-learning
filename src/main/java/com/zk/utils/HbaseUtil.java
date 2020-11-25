package com.zk.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;


/**
 * @author zhangkai
 * @create 2020/1/15
 */
public class HbaseUtil implements Serializable {
    static Connection connection ;
    static Admin admin;

    public Connection getCon(String quorum,String port) throws Exception{

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum",quorum);
        conf.set("hbase.zookeeper.property.clientPort",port);
        connection = ConnectionFactory.createConnection(conf);
        admin = connection.getAdmin();
        return  connection;
    }


    public TableName[] getAllTables(String namespace)throws Exception{
        return admin.listTableNamesByNamespace(namespace);
    }

    public List<TableName> getAllTables()throws Exception{
        return Arrays.asList(admin.listTableNames());
    }


}

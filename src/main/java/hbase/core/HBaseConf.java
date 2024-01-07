package hbase.core;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * @Author: Mingyang Ma
 * @Date: 2024/1/6 19:16
 * @Version: 1.0
 * @Function:
 */
public class HBaseConf {
    public static Configuration configuration;
    public static Connection connection;
    public static Admin admin;

    public HBaseConf() {
        createConf();
        createAdmin();
    }

    /**
     * 创建 HBase conf
     */
    public void createConf() {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "node1:2181,node2:2181,node3:2181");
        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            System.err.println("Failed to connect to HBase.");
            e.printStackTrace();
        }
    }

    /**
     * 创建 HBase admin
     */
    public void createAdmin() {
        try {
            admin = connection.getAdmin();
        } catch (IOException e) {
            System.err.println("Failed to admin to HBase.");
            e.printStackTrace();
        }
    }

    /**
     * 获取 HBase conf
     */
    public Connection getConnection() {
        return connection;
    }

    /**
     * 获取 HBase admin
     */
    public Admin getAdmin() {
        return admin;
    }
}

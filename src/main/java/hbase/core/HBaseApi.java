package hbase.core;

import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;

import java.util.ArrayList;

/**
 * @Author: Mingyang Ma
 * @Date: 2024/1/6 19:16
 * @Version: 1.0
 * @Function:
 */
public class HBaseApi {
    public static Connection connection;
    public static Admin admin;

    public HBaseApi() {
        connection = new HBaseConf().getConnection();
        admin = new HBaseConf().getAdmin();
    }

    /**
     * select * from table;
     *
     * @param table
     * @return ArrayList<ArrayList < String>>
     */
    public ArrayList<ArrayList<String>> getSelectAll(String table) {
        return new HgetFunction().getSelectAll(connection, table);
    }

    /**
     * sout : select * from table;
     *
     * @param table
     */
    public ArrayList<String> getFamilysColumns(String table) {
        return new HgetFunction().getFamilysColumns(connection, table);
    }
}

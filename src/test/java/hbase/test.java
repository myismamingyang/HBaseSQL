package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;

/**
 * @Author: Mingyang Ma
 * @Date: 2024/1/6 22:38
 * @Version: 1.0
 * @Function:
 */
public class test {
    public static Configuration configuration;
    public static Connection connection;
    public static Admin admin;

    public static void main(String[] args) throws IOException {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "node1:2181,node2:2181,node3:2181");
        connection = ConnectionFactory.createConnection(configuration);
        ArrayList<String> test = getFamilysColumns(connection, "test");
        for (String fc : test) {
            System.out.println(fc);
        }
    }

    /**
     * 获取列簇名
     *
     * @param connection
     * @param table
     * @return
     */
    public static ArrayList<String> getFamilys(Connection connection, String table) {
        ArrayList<String> familys = new ArrayList<>();
        Table htable = null;
        try {
            TableName tableName = TableName.valueOf(table);
            htable = connection.getTable(tableName);
            HTableDescriptor tableDescriptor = htable.getTableDescriptor();
            HColumnDescriptor[] Families = tableDescriptor.getColumnFamilies();
            for (HColumnDescriptor Family : Families) {
                System.out.println("Column Family: " + Bytes.toString(Family.getName()));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new ArrayList<>(new HashSet<>(familys));
    }

    public static ArrayList<String> getFamilysColumns(Connection connection, String table) {
        ArrayList<String> familysColumns = new ArrayList<>();
        try {
            HTable hTable = (HTable) connection.getTable(TableName.valueOf(table));
            Scan scan = new Scan();
            scan.setMaxVersions();
            scan.setBatch(1000);
            ResultScanner rs = hTable.getScanner(scan);
            for (Result r : rs) {
                Cell[] cells = r.rawCells();
                for (Cell c : cells) {
                    familysColumns.add(new String(CellUtil.cloneFamily(c)) + ":" + new String(CellUtil.cloneQualifier(c)));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        familysColumns = new ArrayList<>(new HashSet<String>(familysColumns));
        Collections.sort(familysColumns);
        return familysColumns;
    }
}
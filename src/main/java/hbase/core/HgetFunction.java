package hbase.core;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;

/**
 * @Author: Mingyang Ma
 * @Date: 2024/1/6 22:09
 * @Version: 1.0
 * @Function:
 */
public class HgetFunction {
    /**
     * select * from table;
     *
     * @param connection
     * @param table
     * @return
     */
    public ArrayList<ArrayList<String>> getSelectAll(Connection connection, String table) {
        ArrayList<ArrayList<String>> tableValue = new ArrayList<>();
        ArrayList<String> rowKeys = getRowKeys(connection, table);
        ArrayList<String> familysColumns = getFamilysColumns(connection, table);
        Table htable = null;
        TableName tableName = null;
        Result result = null;
        try {
            tableName = TableName.valueOf(table);
            htable = connection.getTable(tableName);
            for (String rowKey : rowKeys) {
                result = htable.get(new Get(Bytes.toBytes(rowKey)));
                ArrayList<String> rowValues = new ArrayList<>();
                for (String fc : familysColumns) {
                    rowValues.add(
                            Bytes.toString(
                                    result.getValue(
                                            Bytes.toBytes(fc.substring(0, fc.indexOf(":")))
                                            , Bytes.toBytes(fc.substring(fc.indexOf(":") + 1))
                                    )
                            )
                    );
                }
                tableValue.add(rowValues);
            }
            htable.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (htable != null) {
                htable = null;
            }
        }
        return tableValue;
    }


    /**
     * 获取 rowkeys
     *
     * @param connection
     * @param table
     * @return
     */
    public ArrayList<String> getRowKeys(Connection connection, String table) {
        ArrayList<String> rowKeys = new ArrayList<>();
        Table htable = null;
        ResultScanner rs = null;
        try {
            Scan scan = new Scan();
            scan.setMaxVersions();
            scan.setBatch(1000);
            htable = connection.getTable(TableName.valueOf(table));
            rs = htable.getScanner(scan);
            for (Result r : rs) {
                rowKeys.add(Bytes.toString(r.getRow()));
            }
            rs.close();
            htable.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (rs != null) {
                rs = null;
            }
            if (htable != null) {
                htable = null;
            }
        }
        return new ArrayList<>(new HashSet<>(rowKeys));
    }

    /**
     * 获取列簇名
     *
     * @param connection
     * @param table
     * @return
     */
    public ArrayList<String> getFamilys(Connection connection, String table) {
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

    /**
     * 获取列簇和列名
     *
     * @param connection
     * @param table
     * @return
     */
    public ArrayList<String> getFamilysColumns(Connection connection, String table) {
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

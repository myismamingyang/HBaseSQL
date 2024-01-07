package hbase.core;

import java.util.ArrayList;

/**
 * @Author: Mingyang Ma
 * @Date: 2024/1/7 16:39
 * @Version: 1.0
 * @Function:
 */
public class Hsout {
    /**
     * sout : select * from table;
     *
     * @param table
     */
    public void soutSelectAll(String table) {
        ArrayList<ArrayList<String>> selectAll = new HBaseApi().getSelectAll(table);
        ArrayList<String> familysColumns = new HBaseApi().getFamilysColumns(table);
        for (String fc : familysColumns) {
            System.out.print(fc + "\t");
        }
        System.out.println();
        for (ArrayList<String> row : selectAll) {
            for (String cell : row) {
                System.out.print(cell == null ? "" : cell + "\t");
            }
            System.out.println();
        }
    }
}

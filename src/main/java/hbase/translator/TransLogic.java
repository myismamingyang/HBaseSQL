package hbase.translator;

import hbase.core.Hsout;

/**
 * @Author: Mingyang Ma
 * @Date: 2024/1/6 19:29
 * @Version: 1.0
 * @Function:
 */
public class TransLogic {
    private String SELECT = "select";
    private String FROM = "from";
    private String WHERE = "where";
    private String CREATE = "create";
    private String DROP = "drop";
    private String DISABLE = "disable";

    /**
     * 翻译入口
     *
     * @param sql
     */
    public void trans(String sql) {
        if (sql.startsWith(SELECT)) {
            selectTrans(sql);
        }
    }

    /**
     * 查询语句转换
     *
     * @param sql
     */
    public void selectTrans(String sql) {
        String table = sql.substring(sql.indexOf(FROM) + 4, sql.indexOf(";")).replaceAll(" +", "");
        if (sql.startsWith("select *")) {
            new Hsout().soutSelectAll(table);
        } else {
            String[] columns = sql.substring(0, sql.indexOf(FROM)).replaceAll(SELECT, "").replaceAll(" +", "").split(",");
        }
    }
}

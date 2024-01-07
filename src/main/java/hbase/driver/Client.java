package hbase.driver;

import hbase.translator.TransLogic;

import java.util.Locale;
import java.util.Scanner;

/**
 * @Author: Mingyang Ma
 * @Date: 2024/1/6 19:27
 * @Version: 1.0
 * @Function:
 */
public class Client {
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        while (true) {
            System.out.println(">");
            String sql = scanner.nextLine();
            new TransLogic().trans(sql.toLowerCase());
        }
    }
}

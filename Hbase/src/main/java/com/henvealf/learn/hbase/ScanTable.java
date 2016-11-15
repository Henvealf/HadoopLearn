package com.henvealf.learn.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * 读数据
 * Created by henvealf on 16-10-19.
 */
public class ScanTable {
    public static void main(String[] args) throws IOException {
        Configuration config = new HBaseConfiguration();
        HBaseAdmin admin = new HBaseAdmin(config);

        TableName tablename = TableName.valueOf("test");

        HTable table = new HTable(config, tablename);
        Get get = new Get(Bytes.toBytes("row1"));
        Result result = table.get(get);
        System.out.println("Get: \n" + result);

        // 扫描器
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        try {
            for (Result scannerResult : scanner) {
                System.out.println("Scan: \n" + scannerResult);
            }
        } finally {
            scanner.close();

        }

        admin.disableTable(tablename);
        admin.deleteTable(tablename);

        table.close();
        admin.close();
    }
}

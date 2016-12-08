package com.henvealf.learn.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import java.io.DataInput;
import java.io.IOException;

/**
 * Created by henvealf on 16-12-6.
 */
public class ResultScannerTest {
    public static void main(String[] args) throws IOException {

        /*Configuration conf = HBaseConfiguration.create();

        // conf.set("hbase.zookeeper.quorum","localhost");
        // conf.set("hbase.zookeeper.property.clientPort","2182");
        // conf.set("zookeeper.znode.parent","/hbase");

        HTable hTable = new HTable(conf,"good_table");

        Scan scan = new Scan();
        ResultScanner scanner = hTable.getScanner(scan);

        for( Result result : scanner )
            System.out.println(result);

        hTable.close();*/

    }
}

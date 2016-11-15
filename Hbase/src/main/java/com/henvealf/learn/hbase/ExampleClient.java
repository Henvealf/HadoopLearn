package com.henvealf.learn.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;


/**
 * 使用Java来操作Hbase
 *
 * Created by henvealf on 16-10-19.
 */
public class ExampleClient {
    public static void main(String[] args) throws IOException {
        // 得到 HBase 的配置
        Configuration conf = HBaseConfiguration.create();
        // 下面代表数据管理， 加表删表
        HBaseAdmin admin = new HBaseAdmin(conf);
        try {
            // 表的名字
            TableName tableName = TableName.valueOf("users");
            // 表的描述
            HTableDescriptor htd = new HTableDescriptor(tableName);
            // 表中一行的描述
            //HColumnDescriptor hcd = new HColumnDescriptor("info");
//            HColumnDescriptor hcd = new HColumnDescriptor("second");

            // 向表描述中添加新的行组
            //htd.addFamily(hcd);
            // HBase中建表

           // admin.createTable(htd);
//            admin.addColumn("test",hcd);
            // 获取所有表的描述
            HTableDescriptor[] tables = admin.listTables();
            if(tables.length != 1 || !Bytes.equals(tableName.getName(),tables[0].getTableName().getName())){
               // throw new IOException("创建表失败");
            }

            // 现在开始向表中插入数据
            // 使用表名，来获取表实例
            HTable table = new HTable(conf, tableName);
            try {
                for ( int i = 1; i <=4; i++) {
                    // row key
                    byte[] row = Bytes.toBytes("row" + i);
                    // 使用它来作为要插入的基本单位，必须放入 row key
                    Put put = new Put(row);
                    byte[] columnFamily = Bytes.toBytes("info");
                    byte[] qualifier = Bytes.toBytes(i);
                    byte[] value = Bytes.toBytes("value" + i);
                    put.add(columnFamily, qualifier, value);
                    table.put(put);
                }
                //Get
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                table.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            admin.close();
        }

    }
}

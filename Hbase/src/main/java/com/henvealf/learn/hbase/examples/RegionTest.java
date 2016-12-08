package com.henvealf.learn.hbase.examples;

import com.henvealf.hbase.util.HbaseOperator;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created by henvealf on 16-12-4.
 */
public class RegionTest {

    public static void main(String[] args) throws IOException {
        HbaseOperator operator = new HbaseOperator();


        operator.createTable("good_table", "good_family", "poor_family");

        for (int i = 0; i < 10; i++) {
            //operator.insertRow("good_table", "001" + i ,"good_family","name", i + "");
        }

        operator.insertRow("good_table", "001" + 0 ,"poor_family","name", "lonely");

        //Cell goodCell = operator.getNewerCell("good_table","001" + 1,"good_family","name");
        Cell poorCell = operator.getNewerCell("good_table","001" + 1,"poor_family","name");

        //System.out.println(Bytes.toString(CellUtil.cloneValue(goodCell)));
        System.out.println(Bytes.toString(CellUtil.cloneValue(poorCell)));
        operator.close();
    }
}

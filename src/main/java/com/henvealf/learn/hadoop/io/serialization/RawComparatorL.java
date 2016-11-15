package com.henvealf.learn.hadoop.io.serialization;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparator;

import java.io.IOException;

/**
 * RawComparator＝＝原始数据比较器，
 * 使用其就不用将序列化反序列化后再比较，可以直接比较序列本身
 * Created by henvealf on 16-9-27.
 */
public class RawComparatorL {
    public static void main(String[] args) throws IOException {
        //　使用RawComparator的工厂方法WritableComparator来得到一个原始数据比较器，
        RawComparator<IntWritable> comparator = WritableComparator.get(IntWritable.class);
        IntWritable int1 = new IntWritable(132);
        IntWritable int2 = new IntWritable(132);
        //　他能够直接比较Writable对象
        System.out.println(comparator.compare(int1,int2));

        byte[] byte1 = MyWritableUtils.serialize(int1);
        byte[] byte2 = MyWritableUtils.serialize(int2);

        // 比较序列化后的字节数组
        System.out.println(comparator.compare(byte1,0,byte1.length, byte2, 0, byte2.length));

    }
}

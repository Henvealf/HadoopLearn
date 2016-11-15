package com.henvealf.learn.hadoop.io.serialization;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.StringUtils;

import java.io.*;

/**
 * 一个序列化工具包
 * Created by henvealf on 16-9-27.
 */
public class MyWritableUtils {

    /**
     * 序列化Writable，返回一个字节数组
     * @param writable
     * @return
     */
    public static byte[] serialize(Writable writable) throws IOException {
        // 内存输出流
        ByteArrayOutputStream out  = new ByteArrayOutputStream();
        DataOutputStream dataOut = new DataOutputStream(out);
        writable.write(dataOut);
        dataOut.close();

        return out.toByteArray();
    }

    /**
     * 将序列反序列化位指定的Writable对象
     * @param writable
     * @param bytes
     * @return　序列
     * @throws IOException
     */
    public static byte[] deserialize(Writable writable, byte[] bytes) throws IOException {
        ByteArrayInputStream in = new ByteArrayInputStream(bytes);
        DataInputStream inData = new DataInputStream(in);
        writable.readFields(inData);
        inData.close();
        return bytes;
    }

    public static void main(String[] args) throws IOException {
        IntWritable intWritable = new IntWritable(163);
        // 进行序列化
        byte[] bytes = serialize(intWritable);
        // 可见int类型被序列化为4个字节
        System.out.println(bytes.length);
        // 序列化后的结果，使用16进制显示～～
        System.out.println(StringUtils.byteToHexString(bytes));

        IntWritable newWritable = new IntWritable();
        deserialize(newWritable,bytes);
        System.out.println(newWritable.get());
    }
}

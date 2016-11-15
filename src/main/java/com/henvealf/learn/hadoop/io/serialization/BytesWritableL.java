package com.henvealf.learn.hadoop.io.serialization;

import org.apache.hadoop.io.*;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

/**
 * 字节可写类，包装了二进制数据。
 * 其序列化格式为，四个字节的头部字段， 指定紧随其后的字节的数目，然后就是字节数据。
 *
 * Created by henvealf on 16-9-27.
 */
public class BytesWritableL {
    public static void main(String[] args) throws IOException {
        BytesWritable b = new BytesWritable(new byte[]{ 3, 6});
        byte[] bytes = MyWritableUtils.serialize(b);
        System.out.println(StringUtils.byteToHexString(bytes));
        // 输出为 000000020306

        ObjectWritable ow;
        // 封装了Writable类型的一维数组
        ArrayWritable arrayWritable;
        // 封装了Writable类型二维数组
        TwoDArrayWritable twoDArrayWritable;
        // 封装了类型为Java基本类型Object一位数组，set()时不需要向下转型
        ArrayPrimitiveWritable arrayPrimitiveWritable;
        // 实现了Map<Writable, Writable>接口，也就是说是一个用来存储键值对都为Writable的Map集合。
        MapWritable mapWritable;
        //
        SortedMapWritable sortedMapWritable;

    }


}

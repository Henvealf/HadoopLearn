package com.henvealf.learn.hadoop.io.serialization;

import org.apache.hadoop.io.*;

import java.io.IOException;

/**
 * 实现了Map<Writable, Writable>接口，也就是说是一个用来存储键值对都为Writable的Map集合。
 * 一个MapWritable对象能够同时持有多种类型的Writable，种类限制为127个.
 * get的时候当然需要想上转型～～
 * Created by henvealf on 16-9-27.
 */
public class MapWritableL {

    public static void main(String[] args) throws IOException {
        MapWritable src = new MapWritable();

        src.put(new IntWritable(1),new Text("hadoop"));
        src.put(new IntWritable(2),new LongWritable(163));
        src.put(new Text("123"),new Text("henvealf"));
        MapWritable dest = new MapWritable();
        WritableUtils.cloneInto(dest,src);
        Text text = (Text) dest.get(new IntWritable(1));
        LongWritable l = (LongWritable) dest.get(new IntWritable(2));
        Text text2 = (Text) dest.get(new Text("123"));
        System.out.println(text);
        System.out.println(l);
        System.out.println(text2);
    }
}

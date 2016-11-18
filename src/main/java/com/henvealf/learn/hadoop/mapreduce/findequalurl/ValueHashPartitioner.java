package com.henvealf.learn.hadoop.mapreduce.findequalurl;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 将 hash 方式改为使用 Value hash
 * Created by henvealf on 16-11-18.
 */
public class ValueHashPartitioner extends Partitioner<IntWritable, Text> {

    @Override
    public int getPartition(IntWritable intWritable, Text text, int i) {
        int parNum = text.hashCode() % i ;
        return parNum > 0 ? parNum : -parNum ;
    }
}

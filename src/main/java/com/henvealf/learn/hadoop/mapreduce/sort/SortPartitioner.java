package com.henvealf.learn.hadoop.mapreduce.sort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by Henvealf on 2016/11/12.
 */
public class SortPartitioner extends Partitioner<IntPair, IntWritable> {
    @Override
    public int getPartition(IntPair intPair, IntWritable intWritable, int i) {
        if(intPair.getFirst() > 220) {
            return 1;
        } else {
            return 0;
        }
    }
}

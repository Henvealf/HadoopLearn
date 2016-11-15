package com.henvealf.learn.hadoop.mapreduce.sort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 *
 * Created by Henvealf on 2016/11/12.
 */
public class SortReducer extends Reducer<IntPair, IntWritable, IntWritable, IntWritable>{
    @Override
    protected void reduce(IntPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int first = key.getFirst();
        for(IntWritable outValue : values) {
            context.write(new IntWritable(first),outValue);
        }
    }
}

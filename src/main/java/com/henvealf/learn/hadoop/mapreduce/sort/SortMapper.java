package com.henvealf.learn.hadoop.mapreduce.sort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Henvealf on 2016/11/12.
 */
public class SortMapper extends Mapper<LongWritable, Text, IntPair, IntWritable>{


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] strs = line.split(" ");
        int first = Integer.parseInt(strs[0]);
        int second = Integer.parseInt(strs[1]);
        IntPair intPair = new IntPair(first, second);

        context.write(intPair,new IntWritable(second));
    }
}

package com.henvealf.learn.hadoop.mapreduce.maxtemperature;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 最大气温的Reducer
 * Created by henvealf on 16-9-22.
 */
public class MaxTemperatureReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int maxValue = Integer.MIN_VALUE;
        System.out.println("123");
        System.err.println("errerr");
        // 求出值中的最大值
        for (IntWritable value : values) {
            maxValue = Math.max(maxValue, value.get());
        }
        //输出
        context.write(key, new IntWritable(maxValue));
    }
}

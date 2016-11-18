package com.henvealf.learn.hadoop.mapreduce.findtop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by henvealf on 16-11-16.
 */

public class FindTopReducer extends Reducer<IntWritable, NullWritable, IntWritable, NullWritable> {
    private List<Integer> maxTopList = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        maxTopList = new LinkedList<>();
        maxTopList.add(Integer.MIN_VALUE);
    }

    @Override
    protected void reduce(IntWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        String val = key.toString();
        // 这里已经不会出现数据格式的错误了
        int num = Integer.parseInt(val);
        FindTopComputer.compute(num, maxTopList);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // 当前文件处理完毕，将列表中数据输出
        for(int num : maxTopList) {
            context.write(new IntWritable(num), NullWritable.get());
        }
    }
}
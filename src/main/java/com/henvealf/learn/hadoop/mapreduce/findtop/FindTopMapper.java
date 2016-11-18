package com.henvealf.learn.hadoop.mapreduce.findtop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by henvealf on 16-11-16.
 */
public class FindTopMapper extends Mapper<LongWritable, Text, IntWritable, NullWritable> {

    enum Count {ERROR_RECORD, SUCCESS_RECORD};

    private List<Integer> maxTopList = null;
    private Counter errCounter = null;
    private Counter sucCounter = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        maxTopList = new LinkedList<>();
        maxTopList.add(Integer.MIN_VALUE);
        // 初始化计数器
        errCounter = context.getCounter(Count.ERROR_RECORD);
        sucCounter = context.getCounter(Count.SUCCESS_RECORD);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String val = value.toString();
        int num = Integer.MIN_VALUE;
        // 判断记录是否有误
        try {
            num = Integer.parseInt(val);
            //System.out.println("输出一个值看一看啊!" + num);
            sucCounter.increment(1);
        } catch (Exception e) {
            errCounter.increment(1);
            return;
        }
        FindTopComputer.compute(num, maxTopList);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // 当前文件处理完毕，将列表中数据输出
        for(int num : maxTopList) {
            System.out.println("map output: " + num) ;
            context.write(new IntWritable(num), NullWritable.get());
        }
        System.out.println("---------------------");
    }

}
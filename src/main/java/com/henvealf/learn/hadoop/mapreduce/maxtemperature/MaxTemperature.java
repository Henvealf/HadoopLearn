package com.henvealf.learn.hadoop.mapreduce.maxtemperature;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 寻找最大温度的运行类,控制Job是如何运行的。
 * Created by henvealf on 16-9-22.
 */
public class MaxTemperature {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if(args.length != 2) {
            System.out.println("参数不对，两个");
            System.exit(-1);
        }
        Job job = new Job();
        //设置运行的类
        job.setJarByClass(MaxTemperature.class);
        job.setJobName("Max Temperature");
        // 文件输入路径
        FileInputFormat.addInputPath(job, new Path(args[0]));
        // 文件输出路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(MaxTemperatureMapper.class);
        job.setReducerClass(MaxTemperatureReducer.class);
        // 指定Mapper的输出类型，不指定则和输入格式相同。
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        System.out.println(job.waitForCompletion(true) ? 0 :1);
    }
}

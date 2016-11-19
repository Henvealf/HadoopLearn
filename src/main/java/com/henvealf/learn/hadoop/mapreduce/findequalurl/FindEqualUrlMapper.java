package com.henvealf.learn.hadoop.mapreduce.findequalurl;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 *
 * Created by henvealf on 16-11-18.
 */
public class FindEqualUrlMapper  extends Mapper<LongWritable, Text,  Text, IntWritable >{

    String fileName = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 获取文件分片
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        Path filePath =  fileSplit.getPath();
        fileName = filePath.getName();
        System.out.println(fileName);
    }

    @Override
    protected void map( LongWritable key, Text value , Context context) throws IOException, InterruptedException {
        //
        if(fileName.contains("url1")) {
            //System.out.println("value: " + value + "     \t1");
            context.write(new Text(value.toString().trim()), new IntWritable(1));
        } else {
            //System.out.println("value: " + value + "     \t2");
            context.write(new  Text(value.toString().trim()), new IntWritable(2));
        }
    }
}

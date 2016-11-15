package com.henvealf.learn.hadoop.mapreduce.maxtemperature;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 得到最大气温的Mapper
 * Created by henvealf on 16-9-22.
 */
public class MaxTemperatureMapper
        extends Mapper<LongWritable, Text, Text, IntWritable> {
    private static final int MISSING = 9999;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        System.out.println("line: "  + line);
        String year = line.substring(15, 19);
        //System.out.println("123");
        int airTemperature;
        if(line.charAt(89) == '+') {    //零上还是零下
            airTemperature = Integer.parseInt(line.substring(90, 93));
        } else {
            airTemperature = Integer.parseInt(line.substring(89,93));
        }

        //String quality = line.substring(92,93);
        if (airTemperature != MISSING) {
            context.write(new Text(year), new IntWritable(airTemperature));
        }
    }
}

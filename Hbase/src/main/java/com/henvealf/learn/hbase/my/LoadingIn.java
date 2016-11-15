package com.henvealf.learn.hbase.my;

import org.apache.commons.lang.ObjectUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Created by henvealf on 16-10-23.
 */
public class LoadingIn extends Configured implements Tool{

    static class LoadingMapper extends Mapper<LongWritable, Text, NullWritable, Put> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String val = value.toString();

            String id = val.substring(0,5);
            String grade = val.substring(5,val.length());
            long gradeL = Long.valueOf(grade);
            Put p = new Put(Bytes.toBytes(id));
            p.add(Bytes.toBytes("info"),Bytes.toBytes("id"),Bytes.toBytes(id));
            p.add(Bytes.toBytes("info"),Bytes.toBytes("grade"),Bytes.toBytes(gradeL));
            context.write(null, p);
        }
    }

    public int run(String[] strings) throws Exception {
        if(strings.length != 1) {
            System.err.println("hahah 1111");
            //return -1;
        }

        Job job = Job.getInstance(getConf(), getClass().getSimpleName());
        job.setJarByClass(getClass());
        FileInputFormat.addInputPath(job, new Path("/usr/my-program/process-data/data.in"));
        job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "users");
        job.setMapperClass(LoadingMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputFormatClass(TableOutputFormat.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(HBaseConfiguration.create(),new LoadingIn(), args);
        System.exit(exitCode);
    }
}

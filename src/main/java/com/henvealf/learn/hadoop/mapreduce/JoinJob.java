package com.henvealf.learn.hadoop.mapreduce;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Created by Henvealf on 2016/11/12.
 */
public class JoinJob extends Configured implements Tool{
    public static String SPLIT_STR = "\001";


    static class JoinMapper extends Mapper<LongWritable, Text, Text, Text> {

        private boolean isFactory = false;  //是不是 工厂表
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String lineStr = value.toString();
            String newKey = null;
            String newValue = null;

            if(lineStr.contains("factoryname address") || lineStr.matches("[a-zA-Z\\s]+?\\s\\d+?")) {
                isFactory = true;
                return;
            }
            if(lineStr.contains("addressID addressname") || lineStr.matches("\\d+\\s?[a-zA-Z\\s]+?") ) {
                isFactory = false;
                return;
            }
            String[] lineStrs = lineStr.split(" ");
            if(isFactory) {
                newKey = lineStrs[1];
                newValue = lineStrs[0] + SPLIT_STR + isFactory;
            } else {
                newKey = lineStrs[0];
                newValue = lineStrs[1] + SPLIT_STR + isFactory;
            }
            context.write(new Text(newKey), new Text(newValue));
        }
    }

    static class JoinReducer extends Reducer<Text, Text, Text, NullWritable> {
        boolean isFirst = true;
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String addressId = key.toString();
            // Text other = null;
            String otherColumnStr = null;
            String outId = key.toString();
            String outFactoryName = null;
            String outAddressName = null;
            for (Text other: values) {
                otherColumnStr = other.toString();
                String[] otherColumns = otherColumnStr.split(SPLIT_STR);
                if(otherColumns.length == 2) {

                    if(otherColumns[1].equals("true")) {            // 工厂表
                        outFactoryName = otherColumns[0];
                    } else if (otherColumns[1].equals("false")) {   // 地址表
                        outAddressName = otherColumns[0];
                    }
                }
            }
            Text outText = new Text(outId + "\t" + outFactoryName + "\t" + outAddressName);
            if(isFirst) {
                context.write(new Text("addressId\tfactoryname\taddressname"),NullWritable.get());
                isFirst = false;
            }
            context.write(outText,NullWritable.get());
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        if(strings.length != 3) {
            System.err.println("参数为3个， 输入1，输入2，输出");
            System.exit(1);
        }
        Job job = Job.getInstance(getConf());
        job.setJarByClass(JoinJob.class);
        job.setJobName("reduce join tow table");

        FileInputFormat.addInputPath(job, new Path(strings[0]));
        FileInputFormat.addInputPath(job, new Path(strings[1]));
        FileOutputFormat.setOutputPath(job, new Path(strings[2]));

        job.setMapperClass(JoinMapper.class);
        job.setReducerClass(JoinReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(2);
        return job.waitForCompletion(true)? 0 : 1;
    }

    public static void main(String args[]) throws Exception {
        int exitCode = ToolRunner.run(new JoinJob(),args);
        System.exit(exitCode);
    }

}

package com.henvealf.learn.hadoop.mapreduce.findequalurl;

import com.henvealf.learn.hadoop.mapreduce.TestJob;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by henvealf on 16-11-18.
 */
public class FindEqualUrlJob extends Configured implements Tool{


    @Override
    public int run(String[] strings) throws Exception {
        if(strings.length != 3) {
            System.err.println("参数为3个：<输入文件一路径> <输入文件二路径> <输出路径>");
            System.exit(1);
        }
        // splitSize = max{minSize, min{maxSize,blockSize}}
        getConf().set("mapred.max.split.size","1");

        Job job = Job.getInstance(getConf());
        // 别忘了这个
        job.setJarByClass(FindEqualUrlJob.class);
        job.setJobName("find equals url job");

        // 单位为字节,现在就是 1Mb
        FileInputFormat.setMaxInputSplitSize(job, 1024 * 1024);

        FileInputFormat.addInputPath(job, new Path(strings[0]));
        FileInputFormat.addInputPath(job, new Path(strings[1]));
        FileOutputFormat.setOutputPath(job, new Path(strings[2]));

        job.setMapperClass(FindEqualUrlMapper.class);
        job.setPartitionerClass(ValueHashPartitioner.class);
        job.setReducerClass(FindEqualUrlReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(5);

        return job.waitForCompletion(true)? 0 : 1;
    }


    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new FindEqualUrlJob(), args);
        System.exit(exitCode);
    }
}

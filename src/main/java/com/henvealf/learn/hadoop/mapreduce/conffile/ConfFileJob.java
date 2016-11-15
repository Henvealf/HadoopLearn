package com.henvealf.learn.hadoop.mapreduce.conffile;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

/**
 * Created by Henvealf on 2016/11/13.
 */
public class ConfFileJob extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        if(strings.length != 3) {
            System.err.println("参数为3个: 配置文件路径 输出路径 输出路径");
            System.exit(1);
        }

        // 设置配置
        FileUtil.setConfFromFile(strings[0],getConf());

        Job job = Job.getInstance(getConf());
        job.setJarByClass(ConfFileJob.class);

        job.setJobName("conf file");

        FileInputFormat.addInputPath(job, new Path(strings[1]));
        FileOutputFormat.setOutputPath(job, new Path(strings[2]));

        job.setMapperClass(ConfFileMapper.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(0);
        return job.waitForCompletion(true)? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new ConfFileJob(),args);
        System.exit(exitCode);
    }
}

package com.henvealf.learn.hadoop.mapreduce.sitelog;

import com.henvealf.learn.hadoop.mapreduce.TestJob;
import org.apache.commons.lang.ObjectUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec;
import sun.security.krb5.internal.PAData;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by henvealf on 16-11-21.
 */
public class CleanLogJob extends Configured implements Tool {


    private static class CleanLogMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        private static final String REGEX = "(.*?) - - \\[(.*?) .*?\\] \"(.*?) (.*?) (.*?)\" (\\d+?) (\\d+|-)";
        private Pattern pattern = null;
        private Matcher m = null;
        private Counter counter = null;
        enum COUNT{ERROR_RECORD};
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            pattern = Pattern.compile(REGEX);
            counter = context.getCounter(COUNT.ERROR_RECORD);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Matcher m = pattern.matcher(value.toString());
            SiteLog siteLog = null;
            if(m.find()) {
                siteLog = new SiteLog();
                siteLog.setIp(m.group(1));
                siteLog.setAccDate(m.group(2));
                siteLog.setMethod(m.group(3));
                String urlStr = m.group(4);
                if(urlStr.startsWith("/static/")
                        || urlStr.startsWith("/data/")
                        || urlStr.startsWith("/uc_server/images")
                        || urlStr.startsWith("/source/plugin")
                        )
                    return;
                siteLog.setUrl(urlStr);
                siteLog.setProtocol(m.group(5));
                siteLog.setStatus(Integer.parseInt(m.group(6)));
                int flowRate = -1;
                if(! m.group(7).contains("-")) {
                    flowRate = Integer.parseInt(m.group(7));
                }
                siteLog.setFlowRate( flowRate );
            }
            //System.out.println(siteLog);
            if(siteLog == null){
                System.out.println("error record: " + key);
                counter.increment(1);
                return;
            }
            context.write(new Text(siteLog.toString()), NullWritable.get());
        }
    }

    public static class CleanLogReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        int argsLength = strings.length;
        if(argsLength < 2) {
            System.err.println("参数至少2个: <输入1>..<输入n> 输出");
            System.exit(1);
        }

        // getConf().set("mapred.max.split.size","2");
        Job job = Job.getInstance(getConf());
        job.setJobName("test job");

        // 别忘了这个
        job.setJarByClass(CleanLogJob.class);

        for(int i = 0; i < argsLength - 1; i ++) {
            FileInputFormat.addInputPath(job, new Path(strings[i]));
        }

        FileOutputFormat.setOutputPath(job, new Path(strings[argsLength - 1]));

        job.setMapperClass(CleanLogMapper.class);
        //job.setReducerClass(CleanLogReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        //job.setNumReduceTasks(1);

        return job.waitForCompletion(true)? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new CleanLogJob(), args);
        System.exit(exitCode);
    }
}

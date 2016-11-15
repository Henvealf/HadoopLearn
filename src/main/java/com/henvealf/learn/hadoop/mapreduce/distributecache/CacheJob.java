package com.henvealf.learn.hadoop.mapreduce.distributecache;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;

/**
 *
 * Created by Henvealf on 2016/11/13.
 */
public class CacheJob extends Configured implements Tool {

    // path hdfs 中的文件路径
    public static String useDistributedCacheBySymbolicLink(Path path) throws IOException {
        FileReader r = new FileReader(path.toString());
        BufferedReader reader = new BufferedReader(r);
        String s = null;
        String str = "";
        while((s = reader.readLine()) != null) {
            str += s;
        }
        r.close();
        reader.close();
        return  str;
    }

    static class CacheMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable One = new IntWritable(1);
        private Text word = new Text();
        private String outStr = null;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            try{
                // 取得缓冲中的内容
                Path[] path = DistributedCache.getLocalCacheFiles(context.getConfiguration());
                outStr = useDistributedCacheBySymbolicLink(path[0]);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(new Text(outStr), One);
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        getConf().set("mapred.create.symlink", "yes");
        DistributedCache.createSymlink(getConf());
        // 添加一个缓存文件
        DistributedCache.addCacheFile(new URI(strings[0] + "#m.link"),getConf());
        Job job = Job.getInstance(getConf());
        job.setJarByClass(CacheJob.class);

        job.setJobName("cache file");

        // 输入输出文件
        FileInputFormat.addInputPath(job, new Path(strings[0]));
        FileOutputFormat.setOutputPath(job, new Path(strings[2]));

        job.setMapperClass(CacheMapper.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(0);
        return job.waitForCompletion(true)? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new CacheJob(),args);
        System.exit(exitCode);
    }
}

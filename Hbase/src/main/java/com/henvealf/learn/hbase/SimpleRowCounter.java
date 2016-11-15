package com.henvealf.learn.hbase;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * 结合 HBase ，一个简单的计行数的程序
 * 数据是从HBase中扫描出来的。
 * Created by henvealf on 16-10-19.
 */
public class SimpleRowCounter extends Configured implements Tool {
    // ImmutableBytesWritable row key的数据类型
    // Result 代表scan读出的中一整行
    static class RowCounterMapper extends TableMapper<ImmutableBytesWritable, Result> {
        public static enum Counters { ROWS }

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            // 只是计数
            context.getCounter(Counters.ROWS).increment(1);
        }
    }

    public int run(String[] strings) throws Exception {
        if(strings.length != 1) {
            System.out.println("11111");
            return -1;
        }

        String tableName = strings[0];
        Scan scan = new Scan();
        // 一个过滤器，意思扫描的时候只扫描第一个 key
        scan.setFilter(new FirstKeyOnlyFilter());

        Job job = new Job(getConf(),getClass().getSimpleName());

        job.setJarByClass(getClass());
        //初始化一个 Table Mapper Job, 之后MapReduce就会去 tableName 指定的表中去数据作为记录。
        TableMapReduceUtil.initTableMapperJob(tableName,    // 表名
                                                scan,       // 扫描器
                                                RowCounterMapper.class,     // Mapper
                                                ImmutableBytesWritable.class,   // 输出类型
                                                Result.class,
                                                job);       // job
        job.setNumReduceTasks(0);   // 没有Reducer
        job.setOutputFormatClass(NullOutputFormat.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(HBaseConfiguration.create(), new SimpleRowCounter(),args);
        System.exit(exitCode);
    }

}

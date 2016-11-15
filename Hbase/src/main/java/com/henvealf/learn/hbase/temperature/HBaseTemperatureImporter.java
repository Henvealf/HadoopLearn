package com.henvealf.learn.hbase.temperature;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * 一个在线的网站，功能是能够实时在线浏览各地气象站的历史信息与当前信息
 * 首先需要加载数据进入 Hbase 中，
 * 如果小数据，可以使用 Hadoop 中内置的工具类。
 *
 * 数据量太大的化，就需要自己编写加载工具了。
 * 简略流程，是先将数据放入 HDFS 中，然后用 MapReduce 放入 HBase 中。
 * Created by henvealf on 16-10-21.
 */
public class HBaseTemperatureImporter extends Configured implements Tool {

    static class HBaseTemperatureMapper<K> extends Mapper<LongWritable, Text, K, Put> {
        private NcdcRecordParser parser = new NcdcRecordParser();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            parser.parse(value.toString());
            if(parser.isValidTemperature()) {
                byte[] rowKey =  RowKeyConverter.makeObservationRowKey(parser.getStationId(),
                                    parser.getObservationDate());
                Put p  = new Put(rowKey);
                p.add(HBaseTemperatureQuery.DATA_COLUMNFAMILY,
                        HBaseTemperatureQuery.AIRTEMP_QUALIFIER,
                        Bytes.toBytes(parser.getAirTemperature()));
                context.write(null,p);

            }
        }
    }

    public int run(String[] strings) throws Exception {
        if(strings.length != 1) {
            System.err.println("Usage: must a input");
            return -1;
        }
        Job job = new Job(getConf(), getClass().getSimpleName());
        // 根据提供的 class 来寻找jar包，即 .class文件 所在的jar包
        job.setJarByClass(getClass());
        FileInputFormat.addInputPath(job, new Path(strings[0]));
        // 配置 map 的输出格式为HBase中表 observations 的格式。
        // 注意这里的 TableOutputFormat 是 org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
        job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "observations");
        job.setMapperClass(HBaseTemperatureMapper.class);
        job.setNumReduceTasks(0);
        // 设置输出格式
        job.setOutputFormatClass(TableOutputFormat.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(HBaseConfiguration.create(), new HBaseTemperatureImporter(),args);
        System.exit(exitCode);
    }

}

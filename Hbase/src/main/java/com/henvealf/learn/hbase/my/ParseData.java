package com.henvealf.learn.hbase.my;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Counter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.List;

/**
 * 一个简单的 MapReduce ,计算最大 grade
 * Created by henvealf on 16-10-24.
 */
public class ParseData extends Configured implements Tool{

    private static class ParseMapper extends TableMapper<Text, LongWritable> {
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {

            String id = Bytes.toString(key.get());

            //String id = Bytes.toString(value.getValue(Bytes.toBytes("info"),Bytes.toBytes("id")));
            byte[] rb = value.getValue(Bytes.toBytes("info"), Bytes.toBytes("grade"));

            //byte[] gradeB = new byte[Long.SIZE / 8];

            //System.out.println("查出的grade长度为： "  + rb.length);
            //Log log = new Log4JLogger();
            //log.info("查出的grade长度为： "  + rb.length);

            // Bytes.putBytes(gradeB, gradeB.length - 3, rb, 0, rb.length);
            /*List<Cell> cellList = value.listCells();
            byte[] gradeB = null;
            for (int i = 0; i < cellList.size(); i ++){
                gradeB = cellList.get(i).getValueArray();
                //idB = cellList.get(i).get
            }*/
            //long grade = Bytes.readAsInt(rb,0,rb.length);
            long grade = Bytes.toLong(rb,0,rb.length);
            context.write(new Text(id), new LongWritable(grade));
        }
    }

    private static class ParseReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        private long max = Integer.MIN_VALUE;
        private Text maxId = new Text("00000");
        int counter = 0;
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

            for (LongWritable grade : values) {
                if(max < grade.get()) {
                    max = grade.get();
                    maxId = new Text(key);
                }
            }
            counter ++;
            if(counter == 50000)
                context.write(maxId,new LongWritable(max));
        }
    }

    public int run(String[] strings) throws Exception {
        String tableName = "users";
        Job job = Job.getInstance(getConf(), "计算最大 grade");
        // 输出路径
        FileOutputFormat.setOutputPath(job, new Path(strings[0]));
        // 下面设置了输入的表
        TableMapReduceUtil.initTableMapperJob(
                                        tableName,
                                        new Scan(),
                                        ParseMapper.class,
                                        Text.class,
                                        LongWritable.class,
                                        job
                                    );
        job.setReducerClass(ParseReducer.class);
        job.setNumReduceTasks(1);
        // 输出格式，文本乎～
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new ParseData(), args);
        System.exit(exitCode);
    }
}

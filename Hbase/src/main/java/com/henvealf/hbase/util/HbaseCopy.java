package com.henvealf.hbase.util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

public class HbaseCopy {
	/**
	 * 使用HBase作为源和目标的MapReduce示例. 
	 * 本示例简单从一个表复制到另一个表, 只有map过程
	 * 
	 * @author hensh
	 * 
	 */
	public static class MyMapper extends
			TableMapper<ImmutableBytesWritable, Put> {

		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			Configuration config = context.getConfiguration();
			String table = config.get("table");
			System.out.println(table);
		}
		
		
		@Override
		public void map(ImmutableBytesWritable row, Result value,
				Context context) throws IOException, InterruptedException {
			// this example is just copying the data from the source table...
			context.write(row, resultToPut(row, value));
		}

		@SuppressWarnings("deprecation")
		private static Put resultToPut(ImmutableBytesWritable key, Result result)
				throws IOException {
			Put put = new Put(key.get());
	
			for (KeyValue kv : result.raw()) {
				put.add(kv);
			}
			return put;
		}

	}
//
	public static void main(String[] args) throws Exception {
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.rootdir", "hdfs://pseudo:9000/hbase");
		config.set("hbase.zookeeper.quorum",
				"pseudo:2181");
		config.set("zookeeper.znode.parent", "/hbase");
		
		String tableName = "hp_test";
		config.set("table", tableName);
		
//		config.set("hp", "Hello!!");
		Job job = Job.getInstance(config, "ExampleReadWrite");
//		Job job = new Job(config, "ExampleReadWrite");
		job.setJarByClass(HbaseCopy.class); 	// class that contains mapper

		Scan scan = new Scan();
		scan.setCaching(1000); 					// 1 is the default in Scan, which will be bad for MapReduce jobs
		scan.setCacheBlocks(false); 			// don't set to true for MR jobs

		
		TableMapReduceUtil.initTableMapperJob(
				tableName,					// input table
				scan,							// Scan instance to control CF and attribute selection
				MyMapper.class,					// mapper class
				null,							// mapper output key
				null,							// mapper output value
				job);
		TableMapReduceUtil.initTableReducerJob("hp_test_copy", // output table
				null,							// reducer class
				job);
		job.setNumReduceTasks(0);
		boolean b = job.waitForCompletion(true);
		if (!b) {
			throw new IOException("error with job!");
		}
	}

}

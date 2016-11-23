/**
 * 一个网站日志分析项目
 * 测试类是 com.henvealf.hadoop.test 中的 SiteParseTest。
 * 1. 对数据进行清洗，正则表达式的说。
 * 2. 使用 mapreduce 进行全部清溪的同时导入到 HDFS 中。
 * 3. 导入到按天来分区的 hive 表 log_items 中。
 * 4. 然后就是分析了
 * Created by henvealf on 16-11-21.
 */
package com.henvealf.learn.hadoop.mapreduce.sitelog;
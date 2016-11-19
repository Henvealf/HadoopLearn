/**
 * 先有两个文件，每行存储的是一些Url，现在的任务是找出两个文件中相同的Url。
 * 主要是利用 partitioner 的功能，将相同的URl分到相同的 Reducer 中，使用默认的 hash 就可以
 * map 端输出 key 为 url ， value 为 IntWritable 类型的标记，当前在文件1，就标为1，否则为2
 * reduce 端，因为从 map 端的数据输入之后会value分组成一个列表。
 * 比如一条 <www.baidu.com,<1,1,2>>
 * 意思就是文件一中有两个 www.baidu.com，
 * 文件二中有 1 个 www.baidu.com，
 * 所以这里只要value集合中1,2两个值同时出现，
 * 就说明该 Url 为两个文件共有的。
 *
 * 注： 生成 Url 的测试方法为 test 包中的 CreateData 类的 create_find_equals_url_data() 方法。
 *
 * Created by henvealf on 16-11-18.
 */
package com.henvealf.learn.hadoop.mapreduce.findequalurl;
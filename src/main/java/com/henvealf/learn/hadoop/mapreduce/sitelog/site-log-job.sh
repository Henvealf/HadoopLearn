#!/bin/bash

# 描述：这是一个日志处理任务集成Shell脚本，
# 作者： Henvealf
#获取昨日日期，暂时先在参数中shu ru ri qi
#yesterday='date --date='1 days ago'+%Y_%m_%d'
yesterday=$1

if ["$yesterday" == "" ]; then 
   echo "请输入日期。"
   exit 1
fi

# 导入数据到 HDFS 中
echo "---导入日志文件到 HDFS 中，从 /usr/my-program/process-data/site-data/access_${yesterday}.log 到 /user/henvealf/site-log\n"
$HADOOP_HOME/bin/hadoop fs -put /usr/my-program/process-data/site-data/access_${yesterday}.log hdfs://localhost:9000/user/henvealf/site-log/

$HADOOP_HOME/bin/hadoop fs -rm -r hdfs://localhost:9000/user/henvealf/site-log-clean/${yesterday}/

# 执行 MapReduce job
echo "---开始执行 MapReduce"
$HADOOP_HOME/bin/hadoop jar /usr/my-program/my-hadoop-jars/hadoop.jar com.henvealf.learn.hadoop.mapreduce.sitelog.CleanLogJob hdfs://localhost:9000/user/henvealf/site-log/access_${yesterday}.log hdfs://localhost:9000/user/henvealf/site-log-clean/${yesterday}/

# 创建表

echo "---创建 hive 的 log_items 表"
hive -e "use vaf;Create Table If Not Exists log_items (ip String,acc_date String,method String,url String,protocal String,status int,flow_rate Int) Partitioned by (acc_day String);"

# 导入清洗后的数据到 hive 表中
echo "---导入清洗后的数据到 log_items 中"
hive -e "use vaf;Load Data InPath '/user/henvealf/site-log-clean/${yesterday}/part-r-00000' Into Table vaf.log_items Partition (acc_day='${yesterday}');"

# PV
echo "---计算 Page View"
hive -e "use vaf;create table if not exists pv_t as select acc_day, count(1) as pv from vaf.log_items where acc_day = '${yesterday}' group by acc_day ;"

# 新用户数
echo "---计算进入新用户数"
hive -e "use vaf;create table if not exists newer_t as select acc_day, count(1) as newer from vaf.log_items where url like '%member.php?mod=register%' and acc_day = '${yesterday}' group by acc_day;"

# 独立ip
echo "---计算独立ip"
hive -e "use vaf;create table if not exists ip_t as  select acc_day, count(distinct ip) as ip from vaf.log_items where acc_day = '${yesterday}' group by acc_day;"

# jumper
echo "---计算 jumper"
hive -e "use vaf;create table if not exists  jumper_t as select acc_day, count(1) jumper  from (select acc_day, ip , count(1) as ip_sum from vaf.log_items group by acc_day, ip) as im where im.ip_sum = 1 and acc_day = '${yesterday}' group by acc_day;"

# 汇总表
echo "---汇总 all data"
hive -e "use vaf;create table if not exists grather_t as select pv_t.acc_day as acc_day, pv, newer, ip, jumper from pv_t JOin newer_t on (pv_t.acc_day = newer_t.acc_day) JOin ip_t on (pv_t.acc_day = ip_t.acc_day) JOin jumper_t on (pv_t.acc_day = jumper_t.acc_day);"

# 导入到 mysql 中
echo "---to mysql"
sqoop export --connect jdbc:mysql://localhost:3306/site_log --username root --password heqwe3QL! --table garther --export-dir /user/henvealf/hive/warehouse/vaf.db/grather_t/000000_0 --input-fields-terminated-by '\001'

echo "---drop all hive template table"
hive -e "use vaf;drop table pv_t;"
hive -e "use vaf;drop table log_items;"
hive -e "use vaf;drop table newer_t;"
hive -e "use vaf;drop table ip_t;"
hive -e "use vaf;drop table jumper_t;"


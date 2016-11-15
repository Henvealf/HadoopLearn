package com.henvealf.learn.hadoop.mapreduce.conffile;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by Henvealf on 2016/11/13.
 */
public class ConfFileMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Map<String, String> cityInfoMap = null;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        cityInfoMap = new HashMap<>();
        Gson gson = new Gson();
        // 获取配置
        Configuration conf = context.getConfiguration();
        String cityGson = conf.get("cityInfos");
        System.err.println("cityGson: " + cityGson);
        cityInfoMap =
                gson.fromJson(cityGson, new TypeToken<Map<String,String>>() { }.getType());

    }

    // 简单的把配置给输出
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        for ( String mapKey : cityInfoMap.keySet() ) {
            System.err.println(mapKey + ":" + cityInfoMap.get(mapKey));
            context.write(new Text(mapKey), new Text(cityInfoMap.get(mapKey)));
        }
    }
}

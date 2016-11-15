package com.henvealf.learn.hadoop.mapreduce.conffile;

import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.web.JsonUtil;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Henvealf on 2016/11/13.
 */
public class FileUtil {
    public static void setConfFromFile(String fileName, Configuration conf) throws IOException {

        BufferedReader reader = new BufferedReader(new FileReader(fileName));
        String lineStr = null;
        Gson gson = new Gson();
        Map<String, String> cityMap = new HashMap<>();
        while((lineStr = reader.readLine()) != null) {
            String[] cityInfo = lineStr.split(" ");
            cityMap.put(cityInfo[0],cityInfo[1]);
        }
        String jsonStr = gson.toJson(cityMap);
        conf.set("cityInfos",jsonStr);
        reader.close();
    }
}

package com.henvealf.learn.hadoop.mapreduce.findequalurl;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by henvealf on 16-11-18.
 */
public class FindEqualUrlReducer extends Reducer<IntWritable, Text, Text, NullWritable> {

    Set<String> url1Set = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        url1Set = new HashSet<>();
    }

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for(Text text : values) {
            String urlStr = text.toString();

            if(key.get() == 1)
                url1Set.add(urlStr);
            else if (key.get() == 2)
                for(String url1Str : url1Set)
                    if(url1Str.equals(urlStr))
                        context.write( new Text(url1Str), NullWritable.get());


        }
    }
}

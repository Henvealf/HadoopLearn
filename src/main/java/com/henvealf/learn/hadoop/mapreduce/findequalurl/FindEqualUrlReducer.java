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
public class FindEqualUrlReducer extends Reducer<Text, IntWritable, Text, NullWritable> {

    //Set<String> url1Set = null;
    //Set<String> url2Set = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //url1Set = new HashSet<>();
        //url2Set = new HashSet<>();
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        boolean meetOne = false;
        boolean meetTow = false;
        for(IntWritable whichFile : values) {
            //System.out.println("key: " + key + "\t value: " + whichFile.toString());
            if (whichFile.get() == 1) {
                // url1Set.add(key.toString());
                meetOne = true;
            } else if (whichFile.get() == 2) {
                //url2Set.add(key.toString());
                meetTow = true;
            }
            if(meetOne && meetTow){
                System.out.println("key: " + key + "\t value: " + whichFile.toString());
                context.write(new Text(key), NullWritable.get());
                break;
            }

        }
    }

}

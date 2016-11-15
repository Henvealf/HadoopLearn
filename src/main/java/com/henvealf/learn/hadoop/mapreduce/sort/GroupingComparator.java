package com.henvealf.learn.hadoop.mapreduce.sort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by Henvealf on 2016/11/12.
 */

public class GroupingComparator extends WritableComparator{

    protected GroupingComparator() {
        super(IntPair.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        IntPair intA = (IntPair) a;
        IntPair intB = (IntPair) b;
        return intA.compareTo(intB);
    }

}

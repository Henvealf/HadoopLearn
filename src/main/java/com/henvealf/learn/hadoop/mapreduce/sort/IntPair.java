package com.henvealf.learn.hadoop.mapreduce.sort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Henvealf on 2016/11/12.
 */
public class IntPair implements WritableComparable<IntPair> {
    private int first;
    private int second;

    public IntPair() {

    }

    public IntPair(int first, int second) {
        this.first = first;
        this.second = second;
    }

    public int getFirst() {
        return first;
    }

    public void setFirst(int first) {
        this.first = first;
    }

    public int getSecond() {
        return second;
    }

    public void setSecond(int second) {
        this.second = second;
    }

    @Override
    public String toString() {
        return first + " " + second;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(first);
        dataOutput.writeInt(second);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        first = dataInput.readInt();
        second = dataInput.readInt();
    }

    @Override
    public int compareTo(IntPair intPair) {

        if(first != intPair.getFirst()) {
            return first - intPair.getFirst();
        } else if(second != intPair.getSecond()) {
            return this.second - intPair.getSecond();
        }
        return 0;
    }

    @Override
    public boolean equals(Object obj) {
        if(obj == null)
            return false;
        if(this == obj)
            return true;
        if(obj instanceof  IntPair) {
            IntPair other = (IntPair) obj;
            if(other.getFirst() == first && other.getSecond() == second)
                return true;
        }
        return false;
    }
}

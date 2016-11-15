package com.henvealf.learn.hadoop.io.serialization;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 一个TextPair的自定义实现，具有两部分的Text类。
 * 继承了WritableComparable方法
 * Created by henvealf on 16-9-27.
 */
public class TextPair implements WritableComparable<TextPair> {

    private Text first;
    private Text second;

    // 必须有一个默认的构造方法
    public TextPair() {
        set(new Text(), new Text());
    }

    public TextPair(String first, String second) {
        set(new Text("first"), new Text("second"));
    }

    public TextPair(Text first, Text second) {
        set(first, second);
    }

    private void set(Text first, Text second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public int compareTo(TextPair o) {
        // 先比较一
        int cmp = first.compareTo(o.first);
        if(cmp != 0) return cmp;
        return second.compareTo(second);
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof TextPair) {
            // 转型
            TextPair tp = (TextPair) obj;
            return first.equals(tp.first) && second.equals(tp.second);
        }
        return false;
    }

    @Override
    public String toString() {
        return first + "\t" + second;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // 将序列写进out中
        first.write(out);
        second.write(out);
    }


    @Override
    public void readFields(DataInput in) throws IOException {
        // 反序列化输入的序列 in
        first.readFields(in);
        first.readFields(in);
    }

    /**
     * HashPartitioner通过调用hashCode()方法，来选择一个reduce分区。
     * @return
     */
    @Override
    public int hashCode() {
        return first.hashCode() * 163  + second.hashCode();
    }

    /**
     * 直接使用WritableComparable的实现类来自定义Writable,他会先将传来的数据反序列化，然后再
     * 使用CompareTo方法来进行排序。这种做法定会影响到效率。
     * 一种替代办法就是通过实现RawComparator来实现自定义。
     */
    public static class Comparator extends WritableComparator {

        private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();

        public Comparator() {
            super(TextPair.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1,
                           byte[] b2, int s2, int l2) {
            try {

                // 使用decodeVIntSize，根据第一个字节得到边长int的长度。
                int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1,s1);
                int firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2,s2);

                int cmp = TEXT_COMPARATOR.compare(b1, s1, firstL1, b2, s2,firstL2);
                if( cmp != 0) {
                    return cmp;
                }
                return TEXT_COMPARATOR.compare(b1, s1 + firstL1, l1 - firstL1,
                                                  b2, s2 + firstL2, l2 - firstL2);
            } catch (IOException e) {
                throw new IllegalArgumentException(e);
            }
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            if(a instanceof TextPair && b instanceof TextPair) {
                return ((TextPair)a).first.compareTo(((TextPair)b).first);
            }
            return super.compare(a, b);
        }
    }

    static {
        WritableComparator.define(TextPair.class, new Comparator());
    }
}

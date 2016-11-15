package com.henvealf.learn.hbase.temperature;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by henvealf on 16-10-23.
 */
public class HBaseTemperatureQuery {
    public static final byte[] DATA_COLUMNFAMILY = Bytes.toBytes("data");
    public static final byte[] AIRTEMP_QUALIFIER = Bytes.toBytes("airtemp");
}

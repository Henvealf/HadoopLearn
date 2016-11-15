package com.henvealf.learn.hbase.temperature;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * 将 气象站ID与时间戳，合并为行键
 * Created by henvealf on 16-10-23.
 */
public class RowKeyConverter {
    private static  int STATION_ID_LENGTH = 32;
    public static byte[] makeObservationRowKey(String stationId, long observationTime) {
        STATION_ID_LENGTH =  Bytes.toBytes(stationId).length;
        byte[] row = new byte[STATION_ID_LENGTH + Bytes.SIZEOF_LONG];
        Bytes.putBytes(row, 0, Bytes.toBytes(stationId), 0, STATION_ID_LENGTH);
        long reverseOrderTimestamp = Long.MAX_VALUE - observationTime;
        Bytes.putLong(row, STATION_ID_LENGTH, reverseOrderTimestamp);
        System.out.println("log row :" + row);
        return row;
    }
}

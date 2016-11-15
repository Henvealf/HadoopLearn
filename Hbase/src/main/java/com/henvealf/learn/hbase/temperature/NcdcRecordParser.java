package com.henvealf.learn.hbase.temperature;

import org.apache.hadoop.io.Text;

import java.util.Date;

public class NcdcRecordParser {
    private static final int MISSING_TEMPERATURE = 9999;

    private String year;
    private int airTemperature;
    private String quality;
    private String stationId;
    private long observationDate;

    public void parse(String record) {
        year = record.substring(15, 19);
        String airTemperatureString;
        // Remove leading plus sign as parseInt doesn't like them
        if (record.charAt(87) == '+') {
            airTemperatureString = record.substring(88, 92);
        } else {
            airTemperatureString = record.substring(87, 92);
        }
        stationId = record.substring(4, 10);
        observationDate = Long.parseLong(record.substring(15,23));
        airTemperature = Integer.parseInt(airTemperatureString);
        quality = record.substring(92, 93);
    }

    public void parse(Text record) {
        parse(record.toString());
    }

    public boolean isValidTemperature() {
        return airTemperature != MISSING_TEMPERATURE
                && quality.matches("[01459]");
    }

    public boolean isMa1formedTemperature() {
        return !quality.matches("[01459]");
    }

    public boolean IsMissingTemperature() {
        return airTemperature == MISSING_TEMPERATURE;
    }

    public String getYear() {
        return year;
    }

    public int getAirTemperature() {
        return airTemperature;
    }

    public String getStationId() {
        return stationId;
    }

    public long getObservationDate() {
        return observationDate;
    }

}
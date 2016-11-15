package com.henvealf.test;

import com.henvealf.learn.hbase.temperature.NcdcRecordParser;
import org.junit.Test;
import  org.junit.*;

/**
 * Created by henvealf on 16-10-23.
 */
public class TestParser {
    @Test
    public void test_parserStationId() {
        NcdcRecordParser ncdcRecordParser = new NcdcRecordParser();
        ncdcRecordParser.parse("0029029070999991901010106004+64333+023450FM-12+000599999V0202701N015919999999N0000001N9-00781+99999102001ADDGF108991999999999999999999");
        String id = ncdcRecordParser.getStationId();
        long date = ncdcRecordParser.getObservationDate();
        assert ("029070".equals(id));
        assert (19010101 == date);
    }
}

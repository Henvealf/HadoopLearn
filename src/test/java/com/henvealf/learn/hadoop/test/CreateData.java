package com.henvealf.learn.hadoop.test;

import org.junit.Test;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;
import java.util.Random;

/**
 * Created by henvealf on 16-11-18.
 */
public class CreateData {

    @Test
    public void create_find_equals_url_data() throws IOException {
        FileWriter writer = new FileWriter("/usr/my-program/process-data/url2.dt");
        Random r = new Random(new Date().getTime());
        for(int i = 0; i < 300000; i ++) {
            String str = "www.henvealf.com/" + r.nextInt(1000000);
            if(i < 299999)
                str += "\n";
            writer.write(str);
            writer.flush();
        }
        writer.close();
    }

}

package com.henvealf.test;

import com.google.common.primitives.Bytes;

import java.io.*;
import java.util.Date;
import java.util.Random;

import static java.lang.System.out;
import static java.lang.System.setOut;

/**
 * Created by henvealf on 16-10-23.
 */
public class MakeData {

    public static void main(String[] args) throws IOException {
        PrintWriter writer = new PrintWriter("/usr/my-program/process-data/data.in");
        Random r = new Random(47);
        for(int i = 0; i < 50000; i ++) {
            String result = String.valueOf(i);

            int add = 5 - result.length();
            for(int j = 0; j < add; j++) {
                result = "0" + result;
            }
            int grade = r.nextInt(100) +1;
            System.out.println("写：" + i + " grade: " + grade);
            writer.println(result + grade);
        }
        writer.close();
    }
}

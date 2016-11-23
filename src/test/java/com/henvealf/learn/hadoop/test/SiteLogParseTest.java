package com.henvealf.learn.hadoop.test;

import com.henvealf.learn.hadoop.mapreduce.sitelog.SiteLog;
import org.junit.Test;

import java.text.ParseException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 网站日志分析项目测试类
 * Created by henvealf on 16-11-21.
 */
public class SiteLogParseTest {

    @Test
    public void test_cleanRegex() {
        String regex = "(.*?) - - \\[(.*?) .*?\\] \"(.*?) (.*?) (.*?)\" (\\d+?) (\\d+|-)";
        String str = "110.52.250.126 - - [30/May/2013:17:38:20 +0800] \"GET /data/cache/style_1_widthauto.css?y7a HTTP/1.1\" 200 1292\n" +
                "27.19.74.143 - - [30/May/2013:17:38:20 +0800] \"GET /static/image/common/hot_1.gif HTTP/1.1\" 200 680\n" +
                "60.223.249.13 - - [30/May/2013:17:38:25 +0800] \"GET /home.php?mod=spacecp&ac=pm&op=checknewpm&rand=1369906703 HTTP/1.1\" 200 -";
        Matcher m = Pattern.compile(regex).matcher(str);
        while(m.find()) {
            for (int i = 1; i <= m.groupCount(); i++) {
                System.out.println(i + ":" + m.group(i));
            }
            System.out.println("----------------------------");
        }
    }

    @Test
    public void test_process_date()  {
        SiteLog siteLog = new SiteLog();
        //System.out.println(siteLog.processDate("30/May/2013:17:38:20"));
    }
}

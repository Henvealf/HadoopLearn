package com.henvealf.learn.hadoop.mapreduce.sitelog;

import javax.xml.crypto.Data;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

/**
 * 日志实体类
 * Created by henvealf on 16-11-21.
 */
public class SiteLog {
    private String ip;          // ip
    private String accDate;     // 访问日期
    private String method;      // 方法 get post...
    private String url;         // 资源链接
    private String protocol;    // 使用协议
    private int status;         // 访问状态
    private int flowRate;      // 流量

    public SiteLog(String ip, String accDate, String method, String url, String protocol, int status, int flowRate) {
        this.ip = ip;
        this.accDate = accDate;
        this.method = method;
        this.url = url;
        this.protocol = protocol;
        this.status = status;
        this.flowRate = flowRate;
    }

    public SiteLog() {

    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getAccDate() {
        return accDate;
    }

    public void setAccDate(String accDate) {
        this.accDate = accDate;
    }

    public String getMethod() {
        return method;
    }

    public void setMethod(String method) {
        this.method = method;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public int getFlowRate() {
        return flowRate;
    }

    public void setFlowRate(int flowRate) {
        this.flowRate = flowRate;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(ip + "\001");
        sb.append(processDate(this.accDate) + "\001");
        sb.append(method + "\001");
        sb.append(url + "\001");
        sb.append(protocol + "\001");
        sb.append(status + "\001");
        sb.append(flowRate);
        return sb.toString();
    }

    private String processDate(String dateStr) {
        if(dateStr == null) {
            return "NULL";
        }
        dateStr = dateStr.trim();
        //System.out.println("dateStr: " + dateStr);
        SimpleDateFormat dateFormat =
                new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH);
        Date date = null;

        try {
            date = dateFormat.parse(dateStr);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
        return dateFormat.format(date);
    }
}

package com.henvealf.learn.hadoop.io.filedatastruc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.net.URI;

/**
 * 将顺序文件读取出来
 * Created by henvealf on 16-9-28.
 */
public class SequenceFileReadDemo {
    public static void main(String[] args) throws IOException {
        String uri = args[0];
        Configuration conf = new Configuration();
        // 得到文件系统的连接？
        FileSystem fs = FileSystem.get(URI.create(uri),conf);
        Path path = new Path(uri);

        SequenceFile.Reader reader = null;

        try {
            // 文件系统， 文件路径， 配置
            reader = new SequenceFile.Reader(fs, path, conf);

            // 根据配置实例化一个Writable。获取Key和value的实例
            Writable key = (Writable)
                    ReflectionUtils.newInstance(reader.getKeyClass(),conf);
            Writable value = (Writable)
                    ReflectionUtils.newInstance(reader.getValueClass(),conf);
            // 设置开始的读取位置
            long position = reader.getPosition();

            while (reader.next(key, value)) {
                String syncSeen = reader.syncSeen() ? "*" : "";
                System.out.printf("[%s%s]\t%s\t%s\n", position,syncSeen, key, value);
                position = reader.getPosition();    //开始下一条记录
            }
        } finally {
            IOUtils.closeStream(reader);
        }
    }

}

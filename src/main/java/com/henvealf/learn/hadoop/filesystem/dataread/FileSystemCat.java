package com.henvealf.learn.hadoop.filesystem.dataread;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

/**
 * 使用FileSystem API读取HDFS中的数据，
 * 如同在URLCat中提到的，有时候调用不到URLStreamHandlerFactory,这时候就可以使用FileSystem了。
 * 在Hadoop文件系统中的文件是使用Path对象来代表的，就像 hdfs://localhost/user/tom/quangle.txt。
 *
 * Created by henvealf on 16-9-23.
 */
public class FileSystemCat {
    public static void main(String[] args) throws IOException {
        String uri = args[0];
        // 实例化一个代表配置的对象，使用默认的配置文件。
        Configuration conf = new Configuration();
        // 得到相应文件系统的引用
        FileSystem fs = FileSystem.get(URI.create(uri),conf);
        InputStream in = null;
        try {
            // 真正的打开文件系统中的文件路径，并得到输出流，实际上是一个FSDataInputStream
            // 关于FSDataInputStream请看FileSystemDoubleCat
            in = fs.open(new Path(uri));
            // 下面和URLCat相同
            IOUtils.copyBytes(in, System.out, 4069, false);
        } finally {
            IOUtils.closeStream(in);
        }
    }
}

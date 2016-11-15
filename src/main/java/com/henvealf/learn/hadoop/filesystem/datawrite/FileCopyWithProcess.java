package com.henvealf.learn.hadoop.filesystem.datawrite;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

import java.io.*;
import java.net.URI;

/**
 * 将本地的文件拷贝到HDFS文件系统中
 *
 *  FSDataOutputStream
 *  FileSystem方法会返回一个FSDataOutputStream,
 *  FSDataOutputStream里有一个能都得到当前文件指针的方法。
 *      long getPos() throws IOException;
 *  他只有这个方法，没有seek方法去任意移动指针。
 *
 *  Directories //目录
 *  FileSystem 提供一个创建目录的方法：
 *  boolean mkdirs(Path f) throws IOException;
 *  这个方法会自动创建不存在的父级目录。如果创建成功就返回true。
 *  不过，你没必要单独来创建不存在的目录，因为create()方法也会自动将不存在的父级目录创建好。
 * Created by henvealf on 16-9-23.
 */
public class FileCopyWithProcess {
    public static void main(String[] args) throws IOException {
        String localSrc = args[0];
        String dst = args[1];

        // 使用Java类库获取本地文件输入流
        InputStream in = new BufferedInputStream(new FileInputStream(localSrc));

        Configuration conf = new Configuration();
        // 获取 FileSystem 对象
        FileSystem fs = FileSystem.get(URI.create(dst),conf);
        // 使用Hadoop的文件系统对象获取输入文件流。
        // 如果目录与文件不存在，就自动创建目录与文件。
        // 一个回调接口，Progressable,能够告诉你，你的文件写入进度。
        OutputStream out = fs.create(new Path(dst), new Progressable() {
            @Override
            public void progress() {
                System.out.print(".");
            }
        });

        // 开始写入
        IOUtils.copyBytes(in, out, 4096, true);

        // 还有一个
        // public FSDataOutputStream append(Path f) throws IOException
        // 用于在已有的文件最后追加内容.可以使用它来生成无边界的文件，例如日志文件。
        // 该方法在并没有在所有的 Hadoop 文件系统中实现，比如HDFS中有，而S3中就没有。


    }
}

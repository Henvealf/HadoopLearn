package com.henvealf.learn.hadoop.filesystem.dataread;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.net.URI;

/**
 * FSDataInputStream的使用：
 * 他是继承于标准IO的DataInputStream。
 * 实现了Seekable接口,于是乎他就能够支持随机存取，
 * 两个方法：
 *      void seek(long pos) throws IOException;
 *      long getPos() throws IOException;
 * 还实现了PositionedReadable接口，所以你可以使用一个偏移量，这样你就能用他来读取到流中的任何部分。
 *      int read(long position, byte[] buffer, int offset, int length)
 *          throws IOException;
 *      void readFully(long	position, byte[] buffer, int offset, int length)
 *          throws IOException;
 *      public void readFully(long position, byte[] buffer)
 *          throws IOException;
 *
 * Created by henvealf on 16-9-23.
 *
 */
public class FileSystemDoubleCat {
    public static void main(String[] args) throws IOException {
        String uri = args[0];
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri) ,conf);
        FSDataInputStream in = null;
        try {
            in = fs.open(new Path(uri));
            IOUtils.copyBytes(in, System.out, 1024, false);
            in.seek(0);
            IOUtils.copyBytes(in, System.out, 1024, false);
            byte[] b3 = new byte[3];
            byte[] b33 = new byte[6];
            // 当都使用这个参数时，可以看出下面两个方法没有区别
            in.read(1,b3,0,3);
            in.readFully(1,b3,0,3);
            // 当只使用这两个参数，就会从position开始读数据，直到填充满Buffer，
            // 当读的过程中遇到EOF，就会抛出异常。
            in.readFully(1,b33);
            System.out.println(new String(b3));
            System.out.println(new String(b33));
        } finally {
            IOUtils.closeStream(in);
        }
    }
}

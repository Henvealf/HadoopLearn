package com.henvealf.learn.hadoop.filesystem.dataread;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

/**
 * 使用java.net.URL API读取HDFS中的数据
 * Created by henvealf on 16-9-23.
 */
public class URLCat {

    /**
     * 因为下面的方法在同一个JVM中只能被调用一次，所以将其放在静态的代码快中就可以。
     * 如果在这之前突然有一个在你控制之外的第三方的组件调用此方法。
     * 你就不能在这样来获取Hadoop中的数据
     */
    static {
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
    }

    public static void main(String[] args) throws IOException {
        InputStream in = null;
        try {
            // 这里注意，如果自己在core-site.xml中指定了端口号，在这里就一定要输入自己指定的端口号
            // "hdfs://localhost:9000/user/henvealf/output4/part-r-00000"
            in = new URL(args[0]).openStream();
            // 使用下面的类的静态方法就能够很方便的将输入流中的数据输出到指定的输出流，这里是屏幕标准输出
            // 第三个参数是设定流的缓冲区的大小，
            // 第四个参数表示在复制数据结束后是否关闭流。
            // 我们在下面自己关闭输入流，而标准输出流并不需要关闭
            IOUtils.copyBytes(in, System.out, 4096, false);
        } finally {
            //最后不管程序有没有正常结束，都要将输入流关闭。
            IOUtils.closeStream(in);
        }

    }
}

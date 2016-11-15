package com.henvealf.learn.hadoop.io.compression;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;

/**
 * 流压缩
 * 运行命令为：
 * echo "Text" |  hadoop \
 *  StreamCompressor \
 *  org.apache.hadoop.io.compress.GzipCodec | gunzip -
 *
 * Created by henvealf on 16-9-26.
 */
public class StreamCompressor {
    public static void main(String[] args) throws ClassNotFoundException, IOException {
        // args[0] 传入 CompressionCodec 的一个实现类，以使用不同的压缩工具或者算法
        // String codecClassname = "org.apache.hadoop.io.compress.GzipCodec";
        String codecClassname = args[0];
        // 获得实现类的 Class 对象
        Class<?> codecClass = Class.forName(codecClassname);
        // 获取配置文件
        Configuration conf = new Configuration();

        // 使用Class对象与配置对象，实例化一个CompressionCodec对象
        CompressionCodec codec
                = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
        // 创建一个压缩流，并指定被压缩的输入流，这里是标准输入流
        CompressionOutputStream out = codec.createOutputStream(System.out);
        // 将输入流拷贝到标准输出流去输出
        IOUtils.copyBytes(System.in, out ,4069, false);
        // 来告诉 compressionOutputStream 已经完成了压缩，但此方法并不会关闭输出流。
        out.finish();

    }
}

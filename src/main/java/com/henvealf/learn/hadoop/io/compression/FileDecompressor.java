package com.henvealf.learn.hadoop.io.compression;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

/**
 * 文件解压。
 * 这里使用 CompressionCodecFactory 类,根据文件的后缀名来推断文件的压缩编码，然后进行相应的解压.
 * Created by henvealf on 16-9-26.
 */
public class FileDecompressor {
    public static void main(String[] args) throws IOException {
        String uri = args[0];
        Configuration conf =  new Configuration();
        // 使用统一资源标识符（URI）和Hadoop配置来获得文件系统的对象。
        FileSystem fs = FileSystem.get(URI.create(uri),conf);

        Path inputPath = new Path(uri);
        // 根据Hadoop配置文件得到压缩编码工厂
        CompressionCodecFactory factory = new CompressionCodecFactory(conf);
        // 然后得到相应压缩编码实现类
        CompressionCodec codec = factory.getCodec(inputPath);

        // 没有匹配到
        if(codec == null) {
            System.err.println("No codec found for " + uri);
            System.exit(1);
        }

        // 删掉后缀名，生成输出文件的额路径
        String outPutUri = CompressionCodecFactory.removeSuffix(uri, codec.getDefaultExtension());

        InputStream in = null;
        OutputStream out = null;

        try {
            // 读取并解压文件
            in = codec.createInputStream(fs.open(inputPath));

            out = fs.create(new Path(outPutUri));
            // 使用配置中的 io.file.buffer.size 属性来拷贝输入到输出
            IOUtils.copyBytes(in, out, conf);
        } finally {
            IOUtils.closeStream(in);
            IOUtils.closeStream(out);
        }

    }

}

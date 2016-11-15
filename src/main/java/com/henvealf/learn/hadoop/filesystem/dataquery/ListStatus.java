package com.henvealf.learn.hadoop.filesystem.dataquery;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

/**
 * 列出一个目录中的文件以及文件夹。
 * 使用SystemFile的listStatus方法
 * 一共重载了四个方法
 * 参数分别为：
 *      Path f > 列出该目录下的所有文件和目录信息
 *      Path f, PathFilter filter > 在列出之前使用过滤器筛选出想要的结果。
 *      Path[] files > 同时列出若干个文件下的文件和目录信息
 *      Path[] files, PathFilter filter >
 * 他们都返回一个 FileStatus[]
 * Created by henvealf on 16-9-24.
 */
public class ListStatus {
    public static void main(String[] args) throws IOException {
        String uri = args[0];
        Configuration conf = new Configuration();
        // 根据路径和配置获得相应的文件系统引用
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        // 一个Path数组
        Path[] paths = new Path[args.length];
        for (int i = 0; i < paths.length; i++) {
            paths[i] = new Path(args[i]);
        }

        // 列出目录下所有文件的状态
        FileStatus[] status = fs.listStatus(paths);
        // 使用工具类将FileStatus[]转换为Path[], 数组
        Path[] listedPaths = FileUtil.stat2Paths(status);
        for (Path p : listedPaths) {
            System.out.println(p);
        }
    }
}

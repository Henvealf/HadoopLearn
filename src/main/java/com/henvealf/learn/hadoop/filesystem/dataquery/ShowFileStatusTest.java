package com.henvealf.learn.hadoop.filesystem.dataquery;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.hdfs.MiniDFSCluster;

import java.io.IOException;
import java.io.OutputStream;

/**
 * 查询文件系统中的文件的元数据，意思是找到某一个文件或者目录，以及他们的状态
 * FileStatus中封装了文件系统中文件与目录的元数据。
 * 如果查找的文件不存在，就会抛出FileNotFoundException异常。
 * 为了处理这种情况，可以在使用文件的时候使用boolean exists(Path path) throws IOException 方法。
 * Created by henvealf on 16-9-23.
 */
public class ShowFileStatusTest {

    /*public static void main(String[] args) throws IOException {
        MiniDFSCluster cluster;
        FileSystem fs ;
        Configuration conf = new Configuration();
        if (System.getProperty("test.build.data") == null) {
            System.setProperty("test.build.data", "/tmp");
        }
        cluster = new MiniDFSCluster.Builder(conf).build();

        fs = cluster.getFileSystem();
        OutputStream out = fs.create(new Path("dir/file"));
        out.write("content".getBytes("UTF-8"));
        out.close();
        System.out.println("------初始化成功------");

        System.out.println("-----查看文件状态------");
        Path file = new Path("dir/file");
        FileStatus stat = fs.getFileStatus(file);
        printFileStatus(stat);


        System.out.println("----查看目录状态-------");
        Path dir = new Path("dir");
        stat = fs.getFileStatus(dir);
        printFileStatus(stat);

        if (fs != null) {
            fs.close();
        }
        if(cluster != null) {
            cluster.shutdown();
        }
        System.out.println("-----关闭资源成功-----");
        //
    }

    public static void printFileStatus(FileStatus stat) {
        System.out.println("stat.getPath().toUri().getPath() > " + stat.getPath().toUri().getPath());
        System.out.println("stat.isDirectory() > " + stat.isDirectory());
        System.out.println("stat.getLen() > " + stat.getLen());
        System.out.println("stat.getModificationTime() > " + stat.getModificationTime());
        System.out.println("stat.getReplication() > " + stat.getReplication());
        System.out.println("stat.getBlockSize() > " + stat.getBlockSize());
        System.out.println("stat.getOwner() > " + stat.getOwner());
        System.out.println("stat.getGroup() > " + stat.getGroup());
        System.out.println("stat.getPermission().toString() > " + stat.getPermission().toString());
    }*/
}

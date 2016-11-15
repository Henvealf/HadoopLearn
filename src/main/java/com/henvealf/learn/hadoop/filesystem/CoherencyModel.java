package com.henvealf.learn.hadoop.filesystem;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.OutputStream;

/**
 * 联结模式
 * 他是用来描述数据读写时的可见性，当你使用HDFS执行一些操作的时候，
 * 其行为或者说结果有时并不会符合你的期望。
 * Created by henvealf on 16-10-14.
 */
public class CoherencyModel {

    FileSystem fs = null;
    Configuration conf;

    public void initFs() throws IOException {
        conf = new Configuration();
        fs = FileSystem.get(conf);
    }

    public void test_visible_of_create_file() throws IOException {
        // 在文件系统的命名空间创建一个文件时，该文件是可见的
        Path p = new Path("p");
        fs.create(p);
        System.out.println("fs.exists(p): " + fs.exists(p));
    }

    public void test_visible_of_create_file_and_write_content() throws IOException {
        // 当创建了一个新的文件，然后往其中写入数据，并不能保证数据一定可见，就算是调用了 flush() 方法
        Path second = new Path("second");
        OutputStream out = fs.create(second);
        out.write("content".getBytes("UTF-8"));
        out.flush();
        // 此处输出的值为 0
        // 一旦超高一个块被写入了数据，那么第一个块将会被一个新的reader看到。
        // 对于随后的块也如此：当前的块被写入时，对其他 reader是不可见的
        System.out.println("fs.getFileLength(): " + fs.getFileStatus(second).getLen());
    }

    public void test_visible_of_after_use_hflush() throws IOException {
        // HDFS 在FSDataOutputStream中提供了一个 hflush() 方法，
        // 使用这个方法可以强制的将缓冲区中的数据刷进 datanode中。
        Path p = new Path("third");
        FSDataOutputStream out = fs.create(p);
        out.write("content".getBytes("UTF-8"));
        // 当hflush() 返回成功，HDFS保证数据已经写进了在写通道中所有的datanode中。
        // 并且对所有新的 reader 都可见。
        // 但是他并不能保证datanode中的数据已经写进了磁盘你中，也有可能放在了内存中。
        // （所以当数据中心掉电时，数据可能会丢失）
        // 为了强有力的保证，需要使用 hsync 来代替。
        out.hflush();
        System.out.println("fs.getFileLength(): " + fs.getFileStatus(p).getLen());
    }

    public void test_visible_by_use_hsync() throws IOException {
        // hsync的行为 和 POSIX 中的 fsync系统很相似：将缓冲区中的内容按照描述提交为一个文件。
        // 比如，使用标准的Java类库写入到本地文件中，这样我们就能保证在刷新流之后能够看带文件中的内容并同步
        Path p = new Path("fourth");
        OutputStream out = fs.create(p);
        out.write("content".getBytes("UTF-8"));
        // 其中使用了hsync()
        out.close();
        System.out.println("fs.getFileLength(): " + fs.getFileStatus(p).getLen());
    }

    public static void main(String[] args) throws IOException {
        int option = Integer.parseInt(args[0]);
        CoherencyModel cm = new CoherencyModel();
        cm.initFs();
        switch (option) {
            case 1:
                cm.test_visible_of_create_file();
                break;
            case 2:
                cm.test_visible_of_create_file_and_write_content();
                break;
            case 3:
                cm.test_visible_of_after_use_hflush();
                break;
            case 4:
                cm.test_visible_by_use_hsync();
                break;
        }
    }

    // 应用程序设计的影响
    // 联结模式是在设计应用程序时需要考虑的因素。
    // 如果不调用 hflush() 和 hsync()，你应该准备好在客户端或者系统失效时丢失块中的数据。
    // 在许多应用程序中，这是不能被接受的，
    // 所以你应该在恰当的位置调用 hflush 方法，比如完成了确定记录数或者字节数的数据写入后。
    // 即使 hflush 被设计为不可过度使用，因为他需要一些开销（hsunc 更多），
    // 所以需要在数据的健壮性和生产量之间做一个衡量。
    // 那在应用程序运行时，怎样才是能被接受的。
    // 合适值能够在你的应用程序运行完 hflush 或者 hsync 后测量得到。


}

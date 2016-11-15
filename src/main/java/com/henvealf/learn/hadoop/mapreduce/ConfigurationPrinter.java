package com.henvealf.learn.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.Map;

/**
 * 在命令行中，你可以使用 hadoop fs -conf 选项来选择你使用配置文件
 * 在Hadoop程序中，也提供了Tool接口，来很简单的进行配置文件的选择。
 *
 * GenericOptionParser 泛型选项分析程序，Tool, ToolRunner
 *
 * Hadoop 提供了一些帮助类去很容易的从命令行中去运行 job
 * GenericOptionParser 能够解释你的命令行，
 * 并且能够将设置相应的Configuration实例放在你的应用程序中。
 * 你一般不需要直接使用 GenericOptionParser，直接实现 Tool
 * 接口并使用ToolRunner来运行，已经是十分方便的。
 * <code>
 *     public interface Tool extends Configuration {
 *          int run(String[] args) throws Exception
 *     }
 * </code>
 *
 * -conf 使用时指定的是本地目录中中的配置文件，会覆盖原有的
 * Created by henvealf on 16-10-15.
 */
public class ConfigurationPrinter extends Configured implements Tool {
    static {
        Configuration.addDefaultResource("hdfs-default.xml");
        Configuration.addDefaultResource("hdfs-site.xml");
        Configuration.addDefaultResource("yarn-default.xml");
        Configuration.addDefaultResource("yarn-site.xml");
        Configuration.addDefaultResource("mapred-default.xml");
        Configuration.addDefaultResource("mapred-site.xml");
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        for(Map.Entry<String, String> entry : conf) {
            System.out.printf("%s=%s\n",entry.getKey(), entry.getValue());
        }
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new ConfigurationPrinter(),args);
        System.exit(exitCode);
    }
}

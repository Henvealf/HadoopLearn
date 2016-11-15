package com.henvealf.learn.hadoop.filesystem.dataquery;

/**
 * 文件匹配，即使用通配符来筛选文件和目录。
 * 这里有两个方法：
 *      FileStatus[] globStatus(Path pathPattern) throws IOException;
 *      FileStatus[] globStatus(Path pathPattern, PathFilter filter) throws IOException;
 * 返回一个FileStatus数组，第一个参数使用通配符来匹配文件，第二个参数使用filter来进一步过滤.
 *
 * Hadoop 支持的通配符为：
 * *
 * ? 匹配单个字符
 * [ab] 符合 {a,b} 集合
 * [^ab] 不符合 {a, b} 的集合
 * [a-b] 一个范围
 * [^a-b] 不在一个范围
 * {a,b} a与b中的其中一个
 * \ 转义字符
 * Created by henvealf on 16-9-24.
 */
public class FilePatterns {
}

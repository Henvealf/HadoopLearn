package com.henvealf.learn.hadoop.filesystem.dataquery;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/**
 * PathFilter接口
 * 使用该接口能够让我们使用正则表达式来筛选出想要的Path。
 *
 *
 *
 * Created by henvealf on 16-9-24.
 *
 *
 */
public class RegexExcludePathFilter implements PathFilter {
    private final String regex;

    public RegexExcludePathFilter(String regex) {
        this.regex = regex;
    }

    @Override
    public boolean accept(Path path) {
        return !path.toString().matches(regex);
    }

    // 使用实例
    // fs.globStatus(new Path("/2007/*/*"), new RegexExcludePathFilter("^.*/2007/12/31$"))
    // 将会得到 /2007/12/13
}

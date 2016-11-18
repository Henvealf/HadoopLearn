/**
 * 先有两个文件，每行存储的是一些Url，现在的任务是找出两个文件中相同的Url。
 * 主要是利用 partitioner 的功能，将相同的URl分到相同的 Reducer 中，使用默认的 hash 就可以
 * map 端用于做标记
 *
 * Created by henvealf on 16-11-18.
 */
package com.henvealf.learn.hadoop.mapreduce.findequalurl;
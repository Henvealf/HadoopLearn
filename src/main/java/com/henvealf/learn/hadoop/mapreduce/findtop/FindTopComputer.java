package com.henvealf.learn.hadoop.mapreduce.findtop;

import org.apache.hadoop.mapreduce.Counter;

import java.util.List;

import static com.henvealf.learn.hadoop.mapreduce.findtop.FindTopJob.MAX_COUNT;

/**
 * Created by henvealf on 16-11-15.
 */
public class FindTopComputer {

    public static void compute (int num, List<Integer> maxTopList) {

        int listSize = maxTopList.size();
        System.out.println("新的数为： " + num);
        // 小于最大个数，就在恰当的位置放置新来的值
        if(listSize < FindTopJob.MAX_COUNT) {
            for(int i = 0; i < listSize; i ++) {
                if(num > maxTopList.get(i)) {
                    maxTopList.add(i, num);
                    break;
                }
            }
        }
        // 等于最大个数，如果出现更大的值，就将排名最后的值挤出,加入新的
        else if (listSize == FindTopJob.MAX_COUNT) {
            for(int i = 0; i < listSize; i ++) {
                if(num > maxTopList.get(i)) {
                    maxTopList.remove(listSize - 1);
                    maxTopList.add(i,num);
                    break;
                }
            }
        }
        System.out.println("当前队列： " + maxTopList);
    }
}

package com.henvealf.learn.hadoop.io.serialization;

import org.apache.hadoop.io.Text;

import java.nio.ByteBuffer;

/**
 * 对Text进行迭代访问
 * Created by henvealf on 16-9-27.
 */
public class TextIterator {
    public static void main(String[] args) {
        Text t  = new Text("\u0041\u00DF\u6771\uD801\udc00");
        // 将Text转为Byte数组编制到ByteBuffer中。
        ByteBuffer buff = ByteBuffer.wrap(t.getBytes(), 0, t.getLength());
        int cp;
        // 如果还存在没有读取过得byte数组 ， 就根据当前的位置读取出int编码类型的文本内容
        while (buff.hasRemaining() && (cp = Text.bytesToCodePoint(buff)) != -1) {
            System.out.println(Integer.toHexString(cp));
        }
    }
}

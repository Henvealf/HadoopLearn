package com.henvealf.learn.hadoop.io.serialization;

import org.apache.hadoop.io.Text;

import java.io.UnsupportedEncodingException;

/**
 * Text 是一种使用UTF-8编码的Writable,相当与String
 * Created by henvealf on 16-9-27.
 */
public class TextL {
    public static void string() throws UnsupportedEncodingException {
        String s = "\u0041\u00DF\u6771\uD801\udc00";
        System.out.println(s);
        System.out.println("s.length(): " + s.length());
        System.out.println("s.getBytes(\"utf-8\").length: " + s.getBytes("utf-8").length);
        System.out.println(s.codePointAt(1));
        System.out.println(s.codePointAt(3));
    }
    public static void main(String[] args) throws UnsupportedEncodingException {

        Text text = new Text("你aaaaaaa");
        // 使用中文时输出33，说明一个中文字符占
        System.out.println(text);

        System.out.println("text.getLength() ： " + text.getLength());

        System.out.println("text.getBytes(“utf-8”).length ：  " + text.getBytes().length);

        System.out.println("text.charAt(3) ： " + text.charAt(3));
        System.out.println("out : " + text.charAt(100));

        System.out.println("看看String:");
        String str = "你猜猜我是不是中文？？";
        byte[] strb = str.getBytes("UTF-8");
        System.out.println(str.length());
        System.out.println(strb.length);

        string();
    }

}

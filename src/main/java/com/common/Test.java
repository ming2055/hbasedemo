package com.common;


import com.geohash.GeoHashLen;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;

import java.util.ArrayList;
import java.util.List;

public class Test {
    public static void main(String[] args) {
//        for (int i = 0; i< 100; i++) {
//            System.out.println(new Util().getHBaseSalt());
//        }
//        char x = '|';
//        System.out.println((int) x);
//        System.out.println(Util.getASCII('-'));
//        for(int i = 0; i < 127; i++) {
//            System.out.println((char) i);
//        }

//        String[] sort = new String[]{"wm5tdt0m"
//                ,"wm4tr8tp"
//                ,"wm4z7m5q"
//                ,"wm4zn7cs"
//                ,"wm5jbgu4"
//                ,"wm5meh8g"
//                ,"wm5qjeys"
//                ,"wm5t73fb"
//                ,"wm4tr1nj"
//                ,"wm4yc95k"
//                ,"wm4yhzq1"
//                ,"wm4zx6q4"
//                ,"wm5jtdeh"
//                ,"wm5mw6pm"
//                ,"wm5mxc2z"
//                ,"wm5nv61w"
//                ,"wm4vykdu"
//                ,"wm5jng0v"
//                ,"wm5p5v7j"
//                ,"wm5q5vhb"};
//        String[] sorted = Util.arraySort(sort);
//        for (String x : sorted) {
//
//            System.out.println(x);
//        }
//
//        double[] WIDTH = new double[]{5009400, 1252300, 156500, 39100, 4900, 1200, 152.9, 38.2, 4.8, 1.2, 0.149, 0.037};
//        double width = 0.025;
//        System.out.println(Util.searchInsert(WIDTH, width));


//        long currentId = 1L;
//
//        System.out.println(Bytes.toBytes(currentId).length);
//        byte [] rowkey = Bytes.add(MD5Hash.getMD5AsHex(Bytes.toBytes(currentId)).substring(0, 8).getBytes(),
//                Bytes.toBytes(currentId));
//
//        String str = Bytes.toString(rowkey);
//        System.out.println(rowkey.length);
//        System.out.println(str);

//        int numBuckets = 200;
//        long timestamp = System.currentTimeMillis();
//        long bucket = timestamp % numBuckets;
//        System.out.println(bucket);
//
//        System.out.println(3%3);
//
//        String[] str =  new String[]{
//                "100aaa",
//                "101aaa",
//                "100bbb",
//                "100abc",
//                "100afc"
//        };
//        String[] sorted = Util.arraySort(str);
//        for (String s : sorted) {
//
//            System.out.println(s);
//        }
//
//        Util.getCurrentTime();
//
//
//       System.out.println(Util.longestCommonPrefix(str));
//
//
//        ArrayList<String> list = new ArrayList<>();
//        list.add("1");
//        list.add("1");
//        list.add("1");
//        Object[] strings = list.toArray();
//        for (Object s:strings
//             ) {
//            System.out.println((String) s);
//        }
//
//
//        String[] strings1 = new String[list.size()];
//        list.toArray(strings1);
//        for (String ss: strings1
//             ) {
//            System.out.println(ss);
//        }
//
//
//        String a = "123456789";
//        System.out.println(a.substring(0,5));
//        System.out.println(a.substring(5));


        String[] str = Util.getHexSpiltKeys();
        for (String s:str
             ) {
            System.out.println(s);
        }
    }
}

package com.hbase.util;

import com.google.gson.Gson;
import org.datanucleus.store.types.backed.ArrayList;

import java.lang.reflect.Array;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;

public class test {
    public static class BagOfPrimitives {
        private int value1 = 1;
        private String value2 = "abc";
        private transient int value3 = 3;

        BagOfPrimitives() {
            // no-args constructor
        }

        BagOfPrimitives(int value1) {
            this.value1 = value1;
        }
    }

    public static String[] arraySort(String[] input) {
        for (int i = 0; i < input.length - 1; i++) {
            for (int j = 0; j < input.length - i - 1; j++) {
                if (input[j].compareTo(input[j + 1]) > 0) {
                    String temp = input[j];
                    input[j] = input[j + 1];
                    input[j + 1] = temp;
                }
            }
        }
        return input;
    }


    public static void main(String[] args) throws InterruptedException {
//        String msisdn = "1064731058136";
//        System.out.println(msisdn.hashCode());
//        System.out.println(System.currentTimeMillis());
//
//        SimpleDateFormat simpleDateFormat1 = new SimpleDateFormat("yyyyMMdd");
//        Date day_id = new Date(System.currentTimeMillis());
//        System.out.println(simpleDateFormat1.format(day_id));
//
//        SimpleDateFormat simpleDateFormat2 = new SimpleDateFormat("yyyyMM");
//        Date month_id = new Date(System.currentTimeMillis());
//        System.out.println(simpleDateFormat2.format(month_id));

//        Long date = new Date().getTime();
//        System.out.println(date);

//
//        Date date=new Date();//取时间
//        Calendar calendar = new GregorianCalendar();
//        calendar.setTime(date);
//        calendar.add(calendar.DATE,-1);//把日期往后增加一天.整数往后推,负数往前移动
//        date=calendar.getTime(); //这个时间就是日期往后推一天的结果
//        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
//        String dateString = formatter.format(date);
//
//        System.out.println(dateString);
//        String[] sort = new String[]{"msisdn"
//                ,"subs_id"
//                ,"province_id"
//                ,"user_type"
//                ,"province_name"
//                ,"region_id"
//                ,"region_name"
//                ,"cust_id"
//                ,"cust_name"
//                ,"binding_flag"
//                ,"white_list_flag"
//                ,"nodirect_open_flag"
//                ,"business_system"
//        };
//        String[] sorted = arraySort(sort);
//        for (String x:sorted) {
//
//            System.out.println(x);
//        }


//        long start_time = System.currentTimeMillis();
//        Thread.sleep(1000);
//        long end_time = System.currentTimeMillis();
//        System.out.println((end_time - start_time)/1000 + "s");

//        int sleepTime = 10*1000;
//        Duration duration = Duration.ofMillis(sleepTime);
//        System.out.println(duration.getSeconds());
//
//        String s = null;
//        assert s == null ;
//        System.out.println("end");
//
//        System.out.println(-7 % 4);


        // Serialization
        Gson gson = new Gson();
        System.out.println(gson.toJson(1));            // ==> 1
        System.out.println(gson.toJson("abcd"));       // ==> "abcd"
        System.out.println(gson.toJson(new Long(10))); // ==> 10
        int[] values = {1};
        System.out.println(gson.toJson(values));       // ==> [1]


        // Deserialization
        int one = gson.fromJson("1", int.class);
//        Integer one = gson.fromJson("1", Integer.class);
//        Long one = gson.fromJson("1", Long.class);
//        Boolean false = gson.fromJson("false", Boolean.class);
        String str = gson.fromJson("\"abc\"", String.class);
        String[] anotherStr = gson.fromJson("[\"abc\"]", String[].class);

        System.out.println(str);
        System.out.println(anotherStr[0]);

        // Object Examples
        // Serialization
        BagOfPrimitives obj = new BagOfPrimitives();
        Gson gson1 = new Gson();
        String json = gson1.toJson(obj);
        System.out.println(json);

        // Deserialization
        BagOfPrimitives obj2 = gson.fromJson(json, BagOfPrimitives.class);
        // ==> obj2 is just like obj
        System.out.println(obj2.value3);

        // 3.Array Examples
        System.out.println("=========Array========");
        Gson gson3 = new Gson();
        BagOfPrimitives[] bagOfPrimitives = new BagOfPrimitives[3];
        bagOfPrimitives[0] = new BagOfPrimitives(1);
        bagOfPrimitives[1] = new BagOfPrimitives(2);
        bagOfPrimitives[2] = new BagOfPrimitives(3);
        int[] ints = {1, 2, 3, 4, 5};
        String[] strings = {"abc", "def", "ghi"};

        // Serialization
        System.out.println(gson3.toJson(bagOfPrimitives));     // ==> [1,2,3,4,5]
        System.out.println(gson3.toJson(strings));  // ==> ["abc", "def", "ghi"]

        // Deserialization
        int[] ints2 = gson.fromJson("[1,2,3,4,5]", int[].class);
        // ==> ints2 will be same as ints

    }
}

package com.gson.demo;

import com.google.common.collect.Lists;
import com.google.gson.Gson;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

public class TestGson {

    public static void main(String[] args) throws InterruptedException {

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

        // 4.Collections Examples
        Gson gson4 = new Gson();
        ArrayList<Integer> ints1 = new ArrayList<>();
        ints1.add(1);
        ints1.add(2);
        ints1.add(3);
        String jsonobject = gson.toJson(ints1);

        HashMap<String, String> hashMap = new HashMap<>();
        hashMap.put("name", jsonobject);
// Serialization
        System.out.println(gson.toJson(hashMap));  // ==> json is [1,2,3,4,5]

// Deserialization
//        Type collectionType = new TypeToken<Collection<Integer>>(){}.getType();
//        Collection<Integer> ints2 = gson.fromJson(json, collectionType);
// ==> ints2 is same as ints
    }
}

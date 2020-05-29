package com.gson.demo;

public class BagOfPrimitives {
    private int value1 = 1;
    private String value2 = "abc";
    public transient int value3 = 3;

    BagOfPrimitives() {
        // no-args constructor
    }

    BagOfPrimitives(int value1) {
        this.value1 = value1;
    }
}

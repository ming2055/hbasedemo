package com.hbase;

import org.junit.Test;

import static org.junit.Assert.*;

public class RowkeyForSearchTest {

    @Test
    public void constructRowkey() {
    }

    @Test
    public void getGeoHashLen() {


        assertEquals(1 ,new RowkeyForSearch().getGeoHashLen(1252301, 0.594));
    }
}
package com.hbase.util;

import com.geohash.GeoHashUtil;

import java.util.Iterator;
import java.util.List;

public class SearchNearby {


    public static void main(String[] args) {
        //搜索附近的距离，单位km
        double radius = 20;
        String tableName = "test_lonLat_region";
        String family = "cf";
        GeoHashUtil currentLocation = new GeoHashUtil(29.55,106.57);
        currentLocation.setHashLength(radius);
        List<String> searchHBaseList = currentLocation.getGeoHashBase32For9();
        Iterator<String> rowKeyIterator = searchHBaseList.iterator();

        HBaseUtil.searchByGet(tableName, "a-wmkc1j6x");

        while (rowKeyIterator.hasNext()) {
//            HBaseUtil.scanByPrefixFilter(tableName, rowKeyIterator.next());
            System.out.println(rowKeyIterator.next());
        }




    }
}

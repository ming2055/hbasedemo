package com.hbase;

import com.common.MD5Util;
import com.common.Util;
import com.geohash.GeoHashLen;
import com.geohash.GeoHashUtil;
import com.postition.Graphics;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

public class RowkeyForSearch implements GeoHashLen {

    /**
     * 1.根据用户选择的区域(多边形)，通过各顶点确定一个矩形区域。
     * 2.根据该区域的width和height确定geohash的长度。通过GeoHashLen，找出一个最小面积的geohash矩形覆盖1中确定的矩形
     * 从而确定geohash长度
     * 3.返回geoHash list，包含9个区域
     *
     * @param lon
     * @param lat
     * @return
     */
    public List<String> getSearchGeoList(double[] lon, double[] lat) {
        //获取能将选择区域覆盖的geohash矩形，选取顶点0的经纬度hash
        Graphics.Rectangle rectangle = new Graphics().drawRectangle(lon, lat);
        GeoHashUtil geoHashUtil = new GeoHashUtil(lat[0], lon[0]);
        geoHashUtil.setHashLength(getGeoHashLen(rectangle.getWidth(), rectangle.getHeight()));
        return geoHashUtil.getGeoHashBase32For9();
    }

    /**
     * @param width  通过drawRectangle描绘的矩形宽度，
     * @param height
     * @return
     */
    @Override
    public int getGeoHashLen(double width, double height) {
        int widthPos = Util.searchInsert(WIDTH, width);
        int heightPos = Util.searchInsert(HEIGHT, height);
        return Math.min(widthPos, heightPos);
    }


    /**
     * rowkey:[aa][yyyyMMddHH][geohash][mmss],aa代表[aa][yyyyMMddHH][geohash列表里几个相同位]
     * @param geoHashList
     * @param time yyyyMMddHHmmss
     */
    public static List<String> createSearchRowKeyList(List<String> geoHashList, String time) {

        String timePre = time.substring(0,10);
        //yyyyMMddHH+取geohash前三位（确保固定不变，查询的时候，可能只取3位长度，需要测试） MD5Hash ,然后取前两位作为rowkey salt
        String md5HashSalt = Objects.requireNonNull(MD5Util.getMD5String(timePre + geoHashList.get(0).substring(0, 3))).substring(0,2);

        String timeEnd = time.substring(10);

        List<String> searchRowKeys = new ArrayList<>();
        for (String geoHashStr: geoHashList) {
            searchRowKeys.add(md5HashSalt + timePre + geoHashStr + timeEnd);
        }
        return searchRowKeys;
    }

    public static void main(String[] args){
        GeoHashUtil geoHashUtil = new GeoHashUtil(29.27,106.57);
        geoHashUtil.setHashLength(12);
        List<String> list = geoHashUtil.getGeoHashBase32For9();
        String time = Util.getCurrentTime();
        List<String> list1 = createSearchRowKeyList(list, time);
        for (String rowkey: list1
             ) {
            System.out.println(rowkey);
        }

    }
}

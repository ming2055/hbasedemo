package com.geohash;

import java.math.BigDecimal;
import java.util.Random;

public class RondomLonLat {
    //经度范围 [-180, 180]
    private double MinLon = -180;
    private double MaxLon = 180;
    //纬度范围[-90， 90]
    private double MinLat = -90;
    private double MaxLat = 90;

    public RondomLonLat() {

    }

    public RondomLonLat(double MinLon, double MaxLon, double MinLat, double MaxLat) {
        this.MinLon = MinLon;
        this.MaxLon = MaxLon;
        this.MinLat = MinLat;
        this.MaxLat = MaxLat;
    }

    /**
     * @return
     * @throws
     * @Title: randomLonLat
     * @Description: 在矩形内随机生成经纬度
     */
    public String[] randomLonLat() {
        Random random = new Random();
        BigDecimal db = new BigDecimal(Math.random() * (MaxLon - MinLon) + MinLon);
        String lon = db.setScale(6, BigDecimal.ROUND_HALF_UP).toString();// 小数后6位
        db = new BigDecimal(Math.random() * (MaxLat - MinLat) + MinLat);
        String lat = db.setScale(6, BigDecimal.ROUND_HALF_UP).toString();
        return new String[]{lon, lat};
    }

    public static void main(String[] args) {
//        randomLonLat();
    }

}

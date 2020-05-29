package com.geohash;

public interface GeoHashLen {
    //width height 单位: m
    double[] WIDTH = new double[]{5009400, 1252300, 156500, 39100, 4900, 1200, 152.9, 38.2, 4.8, 1.2, 0.149, 0.037};
    double[] HEIGHT = new double[]{4992600, 624100, 156000, 19500, 4900, 609.4, 152.4, 19, 4.8, 0.595, 0.149, 0.019};

    public abstract int getGeoHashLen(double width, double height);

}

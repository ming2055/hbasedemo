package com.geohash;


import java.util.*;

/**
 * @author wuming
 * @date on 2019/07/14
 */
public class GeoHashUtil {
    private LocationBean location;
    /**
     * 1 2500km;2 630km;3 78km;4 30km
     * 5 2.4km; 6 610m; 7 76m; 8 19m
     */
    private int hashLength = 8; //经纬度转化为geohash长度
    private int latLength = 20; //纬度转化为二进制长度
    private int lngLength = 20; //经度转化为二进制长度

    private double minLat;//每格纬度的单位大小
    private double minLng;//每个经度的大下
    private static final char[] CHARS = {'0', '1', '2', '3', '4', '5', '6', '7',
            '8', '9', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'j', 'k', 'm', 'n',
            'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z'};

    public GeoHashUtil(double lat, double lng) {
        location = new LocationBean(lat, lng);
        setMinLatLng();
    }

    public int gethashLength() {
        return hashLength;
    }

    /**
     * @Author:lulei
     * @Description: 设置经纬度的最小单位，根据此可以求出周围8区块的Geohash
     */
    private void setMinLatLng() {
        minLat = LocationBean.MAXLAT - LocationBean.MINLAT;
        for (int i = 0; i < latLength; i++) {
            minLat /= 2.0;
        }
        minLng = LocationBean.MAXLNG - LocationBean.MINLNG;
        for (int i = 0; i < lngLength; i++) {
            minLng /= 2.0;
        }
    }

    /**
     * @return
     * @Author:lulei
     * @Description: 求所在坐标点及周围点组成的九个
     */
    public List<String> getGeoHashBase32For9() {
        double leftLat = location.getLat() - minLat;
        double rightLat = location.getLat() + minLat;
        double upLng = location.getLng() - minLng;
        double downLng = location.getLng() + minLng;
        List<String> base32For9 = new ArrayList<String>();
        //左侧从上到下 3个
        String leftUp = getGeoHashBase32(leftLat, upLng);
        if (!(leftUp == null || "".equals(leftUp))) {
            base32For9.add(leftUp);
        }
        String leftMid = getGeoHashBase32(leftLat, location.getLng());
        if (!(leftMid == null || "".equals(leftMid))) {
            base32For9.add(leftMid);
        }
        String leftDown = getGeoHashBase32(leftLat, downLng);
        if (!(leftDown == null || "".equals(leftDown))) {
            base32For9.add(leftDown);
        }
        //中间从上到下 3个
        String midUp = getGeoHashBase32(location.getLat(), upLng);
        if (!(midUp == null || "".equals(midUp))) {
            base32For9.add(midUp);
        }
        String midMid = getGeoHashBase32(location.getLat(), location.getLng());
        if (!(midMid == null || "".equals(midMid))) {
            base32For9.add(midMid);
        }
        String midDown = getGeoHashBase32(location.getLat(), downLng);
        if (!(midDown == null || "".equals(midDown))) {
            base32For9.add(midDown);
        }
        //右侧从上到下 3个
        String rightUp = getGeoHashBase32(rightLat, upLng);
        if (!(rightUp == null || "".equals(rightUp))) {
            base32For9.add(rightUp);
        }
        String rightMid = getGeoHashBase32(rightLat, location.getLng());
        if (!(rightMid == null || "".equals(rightMid))) {
            base32For9.add(rightMid);
        }
        String rightDown = getGeoHashBase32(rightLat, downLng);
        if (!(rightDown == null || "".equals(rightDown))) {
            base32For9.add(rightDown);
        }
        return base32For9;
    }

    /**
     * @param length
     * @return
     * @Author:lulei
     * @Description: 设置经纬度转化为geohash长度
     */
    public boolean setHashLength(int length) {
        if (length < 1) {
            return false;
        }
        hashLength = length;
        latLength = (length * 5) / 2;
        if (length % 2 == 0) {
            lngLength = latLength;
        } else {
            lngLength = latLength + 1;
        }
        setMinLatLng();
        return true;
    }

    /**
     * 获取距离有效位数
     * @param radius 单位m
     * @return 当返回值为0，需要另外输入radius
     */
     private int getEffectNum(double radius){
        int result = 0;
        if (radius <= 0) result = 0;
        else if (radius < 0.019) result = 12;
        else if (radius < 0.149) result = 11;
        else if (radius < 0.595) result = 10;
        else if (radius < 4.8) result = 9;
        else if (radius < 19) result = 8;
        else if (radius < 152.4) result = 7;
        else if (radius < 609.4) result = 6;
        else if (radius < 4900) result = 5;
        else if (radius < 19500) result = 4;
        else if (radius < 156000) result = 3;
        else if (radius < 624100) result = 2;
        else if (radius < 4992600) result = 1;
        else result = 0;
        return result;
    }
    /**
     *
     * @param radius 根据radius确定geohash值的字符串长度
     */
    public void setHashLength(double radius){
        int geoHashLength = getEffectNum(radius);
        if (! setHashLength(geoHashLength)) {
            System.err.println("请重新输入要搜索的范围");
        };
    }
    /**
     * @return
     * @Author:lulei
     * @Description: 获取经纬度的base32字符串
     */
    public String getGeoHashBase32() {
        return getGeoHashBase32(location.getLat(), location.getLng());
    }

    /**
     * @param lat
     * @param lng
     * @return
     * @Author:lulei
     * @Description: 获取经纬度的base32字符串
     */
    private String getGeoHashBase32(double lat, double lng) {
        boolean[] bools = getGeoBinary(lat, lng);
        if (bools == null) {
            return null;
        }
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < bools.length; i = i + 5) {
            boolean[] base32 = new boolean[5];
            for (int j = 0; j < 5; j++) {
                base32[j] = bools[i + j];
            }
            char cha = getBase32Char(base32);
            if (' ' == cha) {
                return null;
            }
            sb.append(cha);
        }
        return sb.toString();
    }

    /**
     * @param base32
     * @return
     * @Author:lulei
     * @Description: 将五位二进制转化为base32
     */
    private char getBase32Char(boolean[] base32) {
        if (base32 == null || base32.length != 5) {
            return ' ';
        }
        int num = 0;
        for (boolean bool : base32) {
            num <<= 1;
            if (bool) {
                num += 1;
            }
        }
        return CHARS[num % CHARS.length];
    }

    /**
     * @param lat
     * @param lng
     * @return
     * @Author:lulei
     * @Description: 获取坐标的geo二进制字符串
     */
    private boolean[] getGeoBinary(double lat, double lng) {
        boolean[] latArray = getHashArray(lat, LocationBean.MINLAT, LocationBean.MAXLAT, latLength);
        boolean[] lngArray = getHashArray(lng, LocationBean.MINLNG, LocationBean.MAXLNG, lngLength);
        return merge(latArray, lngArray);
    }

    /**
     * @param latArray
     * @param lngArray
     * @return
     * @Author:lulei
     * @Description: 合并经纬度二进制
     */
    private boolean[] merge(boolean[] latArray, boolean[] lngArray) {
        if (latArray == null || lngArray == null) {
            return null;
        }
        boolean[] result = new boolean[lngArray.length + latArray.length];
        Arrays.fill(result, false);
        for (int i = 0; i < lngArray.length; i++) {
            result[2 * i] = lngArray[i];
        }
        for (int i = 0; i < latArray.length; i++) {
            result[2 * i + 1] = latArray[i];
        }
        return result;
    }

    /**
     * @param value
     * @param min
     * @param max
     * @return
     * @Author:lulei
     * @Description: 将数字转化为geohash二进制字符串
     */
    private boolean[] getHashArray(double value, double min, double max, int length) {
        if (value < min || value > max) {
            return null;
        }
        if (length < 1) {
            return null;
        }
        boolean[] result = new boolean[length];
        for (int i = 0; i < length; i++) {
            double mid = (min + max) / 2.0;
            if (value > mid) {
                result[i] = true;
                min = mid;
            } else {
                result[i] = false;
                max = mid;
            }
        }
        return result;
    }

    class LocationBean {
        public static final double MINLAT = -90;
        public static final double MAXLAT = 90;
        public static final double MINLNG = -180;
        public static final double MAXLNG = 180;
        private double lat;//纬度[-90,90]
        private double lng;//经度[-180,180]

        public LocationBean(double lat, double lng) {
            this.lat = lat;
            this.lng = lng;
        }

        public double getLat() {
            return lat;
        }

        public void setLat(double lat) {
            this.lat = lat;
        }

        public double getLng() {
            return lng;
        }

        public void setLng(double lng) {
            this.lng = lng;
        }
    }

    public static void main(String[] args) {
        // TODO Auto-generated method stub
        GeoHashUtil g = new GeoHashUtil(45.25, 125.29);
        g.setHashLength(24);
        System.out.println("now: " + g.getGeoHashBase32());
        for (String str:
            g.getGeoHashBase32For9()) {
            System.out.println(str);
        }

        System.out.println("=======ChongQing Geohash of lat and lon ");
        double[] latLon = new double[2];
        Map<String, String> geoHashOfCQ = new HashMap();
        geoHashOfCQ.put("渝中", new GeoHashUtil(29.55,106.57).getGeoHashBase32());
        geoHashOfCQ.put("渝北", new GeoHashUtil(29.72,106.63).getGeoHashBase32());
        geoHashOfCQ.put("江北", new GeoHashUtil(29.60,106.57).getGeoHashBase32());
        geoHashOfCQ.put("沙坪坝", new GeoHashUtil(29.53,106.45).getGeoHashBase32());
        geoHashOfCQ.put("南岸", new GeoHashUtil(29.52,106.57).getGeoHashBase32());
        geoHashOfCQ.put("九龙坡", new GeoHashUtil(29.50,106.50).getGeoHashBase32());
        geoHashOfCQ.put("北碚", new GeoHashUtil(29.80,106.40).getGeoHashBase32());
        geoHashOfCQ.put("大渡口", new GeoHashUtil(29.48,106.48).getGeoHashBase32());
        geoHashOfCQ.put("巴南", new GeoHashUtil(29.38,106.52).getGeoHashBase32());
        geoHashOfCQ.put("重庆", new GeoHashUtil(29.57,106.55).getGeoHashBase32());
        for (Map.Entry entry: geoHashOfCQ.entrySet()) {
            System.out.println("location: " + entry.getKey() + ", " + "geohash: " + entry.getValue());
        }
    }
}
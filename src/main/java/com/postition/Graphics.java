package com.postition;

import java.util.Arrays;

public class Graphics {


    public class Rectangle {
        private double topLat;
        private double bottomLat;
        private double leftLon;
        private double rightLon;


        public Rectangle() {

        }

        public double getWidth() {
            return rightLon - leftLon;
        }


        public double getHeight() {
            return topLat - bottomLat;
        }


        public double getTopLat() {
            return topLat;
        }

        public void setTopLat(double topLat) {
            this.topLat = topLat;
        }

        public double getBottomLat() {
            return bottomLat;
        }

        public void setBottomLat(double bottomLat) {
            this.bottomLat = bottomLat;
        }

        public double getLeftLon() {
            return leftLon;
        }

        public void setLeftLon(double leftLon) {
            this.leftLon = leftLon;
        }

        public double getRightLon() {
            return rightLon;
        }

        public void setRightLon(double rightLon) {
            this.rightLon = rightLon;
        }
    }

    /**
     * 根据选中的区域的顶点，确定一个矩形区域，覆盖选中的区域
     *
     * @param lon
     * @param lat
     */
    public Rectangle drawRectangle(double[] lon, double[] lat) {
        Rectangle rectangle = new Rectangle();
        Arrays.sort(lon);
        Arrays.sort(lat);
        rectangle.setBottomLat(lat[0]);
        rectangle.setTopLat(lat[lat.length - 1]);
        rectangle.setLeftLon(lon[0]);
        rectangle.setRightLon(lon[lon.length - 1]);
        return rectangle;
    }
}

package com.common;

import org.apache.calcite.util.Static;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Random;

public class Util {

    public static String getRandomNumber() {
        String ranStr = Math.random() + "";
        int pointIndex = ranStr.indexOf(".");
        return ranStr.substring(pointIndex + 1, pointIndex + 3);
    }

    public static char getHBaseSalt() {
        Random random = new Random();
        int rn = random.nextInt(3);
        char salt = (char) (rn + 'a');
        return salt;
    }

    public static int getASCII(char c) {
        return (int) c;
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

    /**
     * @param A:      an integer sorted array desc
     * @param target: an integer to be inserted
     * @return: An integer
     */
    public static int searchInsert(double[] A, double target) {
        // write your code here
        if (A == null || A.length == 0) {
            return 0;
        }
        int start = 0, end = A.length - 1;

        while (start + 1 < end) {
            int mid = start + (end - start) / 2;
            if (A[mid] == target) {
                return mid;
            } else if (A[mid] < target) {
                end = mid;
            } else {
                start = mid;
            }
        }

        if (A[start] <= target) {
            return start;
        } else if (A[end] <= target) {
            return end;
        } else {
            return end + 1;
        }
    }


    /**
     * @return yyyyMMddHHmmss
     */
    public static String getCurrentTime() {
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
        LocalDateTime date = LocalDateTime.now();
        return date.format(dateTimeFormatter);
    }

    /**
     * @param strs
     * @return
     */
    public static String longestCommonPrefix(String[] strs) {
        if (strs == null || strs.length == 0) {
            return "";
        }
        String prefix = strs[0];
        for (int i = 1; i < strs.length; i++) {
            int j = 0;
            while (j < strs[i].length() && j < prefix.length() && strs[i].charAt(j) == prefix.charAt(j)) {
                j++;
            }
            if (j == 0) {
                return "";
            }
            prefix = prefix.substring(0, j);
        }
        return prefix;
    }


    /**
     * @param list
     * @return
     */
    public static String[] list2Array(List<String> list) {
        String[] strArrary = new String[list.size()];
        list.toArray(strArrary);
        return strArrary;
    }


    public static String[] getHexSpiltKeys() {
        /**
         * 定义char数组,16进制对应的基本字符
         */
        final char[] HEX_DIGITS = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd',
                'e', 'f'};
        String[] splitKeys = new String[255];
        int k = 0;
        StringBuffer key = new StringBuffer();
        for (int i = 0; i < HEX_DIGITS.length; i++) {
            for (int j = 0; j < HEX_DIGITS.length; j++) {
                if ((i == HEX_DIGITS.length - 1) && (j == HEX_DIGITS.length - 1)) continue;
                key.append(HEX_DIGITS[i]);
                key.append(HEX_DIGITS[j]);
                key.append("|");
                splitKeys[k] = key.toString();
                key.setLength(0);
                k++;
            }
        }
        return splitKeys;
    }
}

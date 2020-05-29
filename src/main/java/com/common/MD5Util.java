package com.common;


import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class MD5Util {

    /**
     * 定义char数组,16进制对应的基本字符
     */
    private static final char[] HEX_DIGITS = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd',
            'e', 'f' };

    /**
     * md5加密
     * @param str 需要加密的数据
     * @return 加密结果
     */
    public static String getMD5String(String str) {
        MessageDigest messageDigest = null;
        try {
            messageDigest = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            return null;
        }
        messageDigest.update(str.getBytes());
        return byteArray2HexString(messageDigest.digest());
    }

    /**
     * MD5加密结果（由byte转换成String）
     * @param bytes md5加密后得到的数组
     * @return md5加密结果
     */
    private static String byteArray2HexString(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(HEX_DIGITS[(b & 0xf0) >> 4]).append(HEX_DIGITS[(b & 0x0f)]);
        }
        return sb.toString();
    }

    /**
     * 测试方法
     * @param args
     * @author sucb
     * @date 2017年7月26日下午5:11:50
     */
    public static void main(String[] args) {
        System.out.println(getMD5String("123456"));
    }
}


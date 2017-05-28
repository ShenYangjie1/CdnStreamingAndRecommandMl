package com.sohu.rdc.inf.cdn.offline;

import java.math.BigInteger;
import java.security.MessageDigest;

/**
 * Created by zengxiaosen on 2017/5/21.
 */
public class MD5Test {
    private static int offset = 8 * 60 * 60;
    public static void main(String[] args) throws Exception{
        String tsInSecond = "1494950400.007";
        String region = "00000";
        String code = "00";

        long b = (long)(Double.valueOf(tsInSecond).doubleValue());
        System.out.println(b);

        int tsKey = genDayTs((long) b);
        System.out.println(tsKey);

        String oldRowKey = tsKey + "00000" + "00";
        MessageDigest md5 = MessageDigest.getInstance("MD5");
        md5.update(oldRowKey.getBytes());
        String newRowKey = new BigInteger(1, md5.digest()).toString(16);
        System.out.println(newRowKey);
    }

    private static int genDayTs(long ts){
        return (int) ((ts + offset) / (60 * 60 * 24));
    }
}

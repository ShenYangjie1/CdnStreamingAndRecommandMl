package com.sohu.rdc.inf.cdn.offline.test;

import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by zengxiaosen on 2017/5/20.
 */
public class test1 {
    public static void main(String[] args){
        String regex = "(.*\\d+:\\d+:\\d+)? ?(\\S+[@| ]\\S+) (\\d+.?\\d+) (\\d+.?\\d+)" +
                " (\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}) (-/\\d+) (\\d+) (\\w{3,6})" +
                " (\\w{3,5}://\\S+) - (\\S+) (\\S+[;|; ]?[^\"]*?[;|; ]?[^\"]*?)" +
                " ([\"]\\S*[\"])( [\"].*[\"])?";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher("18:51:02:18:51 local_nginx@tc_128_23 1484736662.031" +
                " 0.001 211.94.173.3 -/200 2201 GET http://images.sohu.com/cs/jsfile/js/fc.js?v=1" +
                " - DIRECT/10.11.128.60:80(0.001) application/x-javascript \"http://inte.sogou.com/" +
                "ct?id=704218&h=280&w=336&fv=0&if=7&sohuurl=http%3A%2F%2Fhuanqiu.chinaiiss.com" +
                "%2Fhtml%2F20171%2F18%2Fa89923.html&refer=http%3A%2F%2Fhuanqiu.chinaiiss.com" +
                "%2Fhtml%2F20171%2F18%2Fa89923.html&rnd=a181cb0b408dbab7&z=fa66269f1d248514&" +
                "lmt=1481605963&srp=1024,768&ccd=32&lhi=0&eja=true&npl=0&nmi=0&ece=true&lan=zh-CN&bi=1" +
                "&t1=955&t2=1484736665&pvt=1484736663807&ssi0=257&ti=&bs=&tmp_cdif=2&mi=2&m" +
                "=MTQ4NDczNjY1OV9wcmV0dHkgZG9nXzcwNDIxOAA-&ex=&glx=0\"");
        if(!matcher.find()){
            System.out.println("no match !!! ");
        }else{
            System.out.println(matcher.group(1));
            System.out.println(matcher.group(2));
            System.out.println(matcher.group(3));
            System.out.println(matcher.group(4));
            System.out.println(matcher.group(5));
            System.out.println(matcher.group(6));
            System.out.println(matcher.group(7));
            System.out.println(matcher.group(8));
            System.out.println(matcher.group(9));
            System.out.println(matcher.group(10));
            System.out.println(matcher.group(11));
            System.out.println(matcher.group(12));
        }

        double tsInSecond = 1484736662.031;
        int inteval_5m = 5 * 60;
        long roundTs_5m = (long) ((Math.floor(tsInSecond/inteval_5m)) * inteval_5m * 1000);
        System.out.println("roundTs_5m: " + roundTs_5m);
        DateTime dateTime = new DateTime(roundTs_5m);
//        DateTime dt_1d = dt_5m.dayOfMonth().roundFloorCopy();
//        System.out.println("dt_1d: " + dt_1d.toString());
//        String result = String.valueOf(dt_5m.getMillis());
//        System.out.println("result: " + result);
        // 下面就是按照一点的格式输出时间
        String str1 = dateTime.toString("yyyy-MM-dd HH:mm:ss");
        String str2 = dateTime.toString("MM/dd/yyyy hh:mm:ss.SSSa");
        String str3 = dateTime.toString("dd-MM-yyyy HH:mm:ss");
        String str4 = dateTime.toString("EEEE dd MMMM, yyyy HH:mm:ssa");
        String str5 = dateTime.toString("MM/dd/yyyy HH:mm ZZZZ");
        String str6 = dateTime.toString("MM/dd/yyyy HH:mm Z");

        System.out.println("str1: " + str1);
        System.out.println("str2: " + str2);
        System.out.println("str3: " + str3);
        DateTimeFormatter format = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");

        LocalDate start = new LocalDate(2012, 12, 14);
        LocalDate end = new LocalDate(2013, 01, 15);

        int days = Days.daysBetween(start, end).getDays();
        System.out.println("days: " + days);


    }
}

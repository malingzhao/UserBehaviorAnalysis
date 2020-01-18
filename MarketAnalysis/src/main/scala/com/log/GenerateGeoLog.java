package com.log;


/*
 * @创建人: MaLingZhao
 * @创建时间: 2020/1/18
 * @描述：
 */

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class GenerateGeoLog {

    //url地址
    public static String[] url_paths = {
            "article/112.html",
            "article/113.html",
            "article/114.html",
            "article/115.html",
            "article/116.html",
            "article/117.html",
            "article/118.html",
            "article/119.html",
            "video/821",
            "tag/list"
    };

    public static String[] cities = {
            "北京", "上海", "广州", "深圳", "大连",
            "哈尔滨", "沈阳", "杭州", "苏州", "三亚"
    };


    //格式化时间样式
    public static String formatTime() {
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return dateFormat.format(new Date());
    }


    //随机生成城市和省份
    public static String sample_province() {
        int urlNum = new Random().nextInt(10);
        return cities[urlNum];
    }


    public static String sample_city() {
        String result = sample_province();
        return result;
    }

    public static Integer sample_userid() {
        Integer s = Utils.getIntUUID(4) + 10000000;
        return s;
    }


    public static Integer sampple_adid() {
        Integer s = Utils.getIntUUID(2) + 10000;
        return s;
    }


    public static long sample_timestamp() {

        long s = System.currentTimeMillis()/1000+ Math.abs((int) Math.round((Math.random()*10+1)*Math.pow(10,3)));;

        return s;
    }

    //生成日志方法
    public static String generateLog() {
        Integer userId = sample_userid();
        Integer adid = sampple_adid();
        String province = sample_province();
        String city = sample_city();

        long timestamp=sample_timestamp();

        String log1 = userId + "," + adid + "," + province + "," + city + "," +timestamp;

        System.out.println(log1);
        return log1;
    }


    //主类
    public static void main(String[] args) throws IOException, InterruptedException {

        //dest生成日志的路径
        String dest = "D:\\workspace\\bigdata\\flink\\UserBehaviorAnalysis\\MarketAnalysis\\src\\main\\scala\\com\\logs\\log.csv";
        File file = new File(dest);

        int num, sleepTime;
        if (args.length == 0) {
            //默认生成日志条数
            num = 1000;
            //默认每10秒生成一次
            sleepTime = 10;
        } else if (args.length == 1) {
            //传一个参数
            num = Integer.valueOf(args[0]);
            sleepTime = 10;
        } else {
            //传两个参数
            num = Integer.valueOf(args[0]);
            sleepTime = Integer.valueOf(args[1]);
        }

        while (true) {
            for (int i = 0; i < num; i++) {


                String content = generateLog() + "\n";
                FileOutputStream fos = new FileOutputStream(file, true);
                fos.write(content.getBytes());
                fos.close();
            }
            //默认多久日志时间
            TimeUnit.SECONDS.sleep(sleepTime);
        }
    }
}













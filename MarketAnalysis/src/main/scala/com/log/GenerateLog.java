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

public class GenerateLog {

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

    //ip数字
    public static String[] ip_splices = {"102","71","145","33","67","54","164","121"};

    //http网址
    public static String[] http_referers = {
            "https://www.baidu.com/s?wd=%s",
            "https://www.sogou.com/web?query=%s",
            "https://cn.bing.com/search?q=%s",
            "https://search.yahoo.com/search?p=%s"
    };

    //搜索关键字
    public static String[] search_keyword = {
            "复制粘贴玩大数据",
            "Bootstrap全局css样式的使用",
            "Elasticsearch的安装（windows）",
            "Kafka的安装及发布订阅消息系统（windows）",
            "window7系统上Centos7的安装",
            "复制粘贴玩大数据系列教程说明",
            "Docker搭建Spark集群（实践篇）"
    };

    //状态码
    public static String[] status_codes = {"200","404","500"};

    //随机生成url
    public static String sample_url(){
        int urlNum = new Random().nextInt(10);
        return url_paths[urlNum];
    }

    //随机生成ip
    public static String sample_ip(){
        int ipNum;
        String ip = "";
        for (int i=0; i<4; i++){
            ipNum = new Random().nextInt(8);
            ip += "."+ip_splices[ipNum];
        }
        return ip.substring(1);
    }

    //随机生成检索
    public static String sample_referer(){
        Random random = new Random();
        int refNum = random.nextInt(4);
        int queryNum = random.nextInt(7);
        if (random.nextDouble() < 0.2){
            return "-";
        }
        String query_str = search_keyword[queryNum];

        String referQuery = String.format(http_referers[refNum], query_str);
        return referQuery;
    }

    //随机生成状态码
    public static String sample_status_code(){
        int codeNum = new Random().nextInt(3);
        return status_codes[codeNum];
    }

    //格式化时间样式
    public static String formatTime(){
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return dateFormat.format(new Date());
    }

    //生成日志方法
    public static String generateLog(){
        String url = sample_url();
        String ip = sample_ip();
        String referer = sample_referer();
        String code = sample_status_code();
        String newTime = formatTime();
        String log = ip+"\t"+newTime+"\t"+"\"GET /"+url+" HTTP/1.1 \""+"\t"+referer+"\t"+code;
        System.out.println(log);
        return log;
    }

    //主类
    public static void main(String[] args) throws IOException, InterruptedException {

        //dest生成日志的路径
        String dest = "D:\\workspace\\bigdata\\flink\\UserBehaviorAnalysis\\MarketAnalysis\\src\\main\\scala\\com\\log\\log.csv" ;
        File file = new File(dest);

        int num, sleepTime;
        if (args.length ==0 ){
            //默认生成日志条数
            num = 1000;
            //默认每10秒生成一次
            sleepTime = 10;
        } else if (args.length == 1){
            //传一个参数
            num = Integer.valueOf(args[0]);
            sleepTime = 10;
        } else {
            //传两个参数
            num = Integer.valueOf(args[0]);
            sleepTime = Integer.valueOf(args[1]);
        }

        while (true){
            for (int i=0; i< num; i++){

                String content = generateLog()+"\n";
                FileOutputStream fos = new FileOutputStream(file, true);
                fos.write(content.getBytes());
                fos.close();
            }
            //默认多久日志时间
            TimeUnit.SECONDS.sleep(sleepTime);
        }
    }
}
package com.log;


/*
 * @创建人: MaLingZhao
 * @创建时间: 2020/1/18
 * @描述：
 */


public class Utils {

    public static Integer getIntUUID(int a) {
        Integer num = Math.abs((int) Math.round((Math.random()*10+1)*Math.pow(10,a)));
        return num;
    }

    static  int num  =0;
    public static  long get_timestamp(){

        num++;

        return  num;
    }

    public static void main(String[] args) {
//        for(int i=0;i<1000;i++) {
//            Integer s = getIntUUID(2);
//            System.out.println(s);
//        }




    for (int i=0;i<1000;i++){
      long  s=  get_timestamp();
        System.out.println(s);
    }
    }
}

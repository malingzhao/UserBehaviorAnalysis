package com.mlz.loginfail_detect

import java.util

import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/*
 * @创建人: MaLingZhao
 * @创建时间: 2020/1/18
 * @描述： 
 */


object LoginFailWithCEP {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env.setParallelism(1)



    /*
    可能会存在
    */


    //1. 读取事件数据，创建事件流

    val logingFialEventStream = env.readTextFile("D:\\workspace\\bigdata\\flink\\UserBehaviorAnalysis\\LoginFailDetect\\src\\main\\scala\\com\\mlz\\loginfail_detect\\logfail.csv")
      .map(data => {
        val dataArray = data.split(",")
        LoginEvent(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(5)) {
        override def extractTimestamp(element: LoginEvent): Long = 1000L
      })
      .keyBy(_.userId) //用户id做分组

    //2. 定义匹配模式
    val loginFailPatterb = Pattern.begin[LoginEvent]("begin").where(_.evrntType=="fail")
        .next("next").where(_.evrntType=="fail")
        .within(Time.seconds(2))


    //3.在事件流上应用模式得到一个pattern strea
    val patternStream= CEP.pattern(logingFialEventStream,loginFailPatterb)



    //4. 从patternStream上应用selectFunction 检测出匹配的序列
    val loginFailDataStream = patternStream.select(new LoginFailMatch())

    loginFailDataStream.print()
    env.execute("login fail  with cep  job")
  }
}







//In Out
class LoginFailMatch() extends  PatternSelectFunction[LoginEvent, Warning]{
  //检测到的所有的事件序列存成一个Map
  override def select(map: util.Map[String, util.List[LoginEvent]]): Warning = {

    //从map中按照名称取出对应的事件
    val firstFail = map.get("begin").iterator().next()

    val lastFail = map.get("next").iterator().next()

    Warning(firstFail.userId,firstFail.eventTime,lastFail.eventTime, "login fail ")

  }
}






/*
* 只要检测到就会输出
* 乱序数据能不能搞定
* 还是检测到了  但是对它是没有影响的
* 复杂的程度上
* cep比前面的模式更简单的
*
* */




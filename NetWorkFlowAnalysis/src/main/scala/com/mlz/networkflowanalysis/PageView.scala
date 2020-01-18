package com.mlz.networkflowanalysis


import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/*
 * @创建人: MaLingZhao
 * @创建时间: 2020/1/17
 * @描述： 
 */


//定义输入数据的样例类
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timeStamp: Long)
//
//
//case class UrlViewCount(url: String, windowEnd: Long, count: Long)

object PageView {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //指定时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)


    val dataStream = env.readTextFile("D:\\workspace\\bigdata\\flink\\UserBehaviorAnalysis\\NetWorkFlowAnalysis\\src\\main\\resources\\UserBehavior.csv")
      .map(data => {
        val dataArray = data.split(",")
        UserBehavior(dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim.toInt, dataArray(3).trim.toString, dataArray(4).trim.toLong)
      })
      .assignAscendingTimestamps(_.timeStamp * 1000L)
      .filter(_.behavior == "pv")
      .map(data => ("pv", 1))
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .sum(1)

    dataStream.print("pv count ")


    env.execute("page view  job ")

  }
}

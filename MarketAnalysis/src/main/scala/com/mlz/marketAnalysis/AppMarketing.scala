package com.mlz.marketAnalysis

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/*
 * @创建人: MaLingZhao
 * @创建时间: 2020/1/17
 * @描述： 
 */


object AppMarketing {

  def main(args: Array[String]): Unit = {


      val env = StreamExecutionEnvironment.getExecutionEnvironment
      env.setParallelism(1)
      env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


      val dataStream = env.addSource(new SimulatedEventSource())
        .assignAscendingTimestamps(_.timestamp)
        .filter(_.behavior != "UNINSTALL")
        .map(data=>{
          ("dummyKey",1L)
        })
        .keyBy(_._1)      //以渠道和行为类型作为key分组
        .timeWindow(Time.hours(1), Time.seconds(10))
        .aggregate(new CountAgg(),new MarktingCountTotal())


      dataStream.print("hello")
      env.execute("app marketing by channel job ")

}

  //In Out 输入 输出
  class CountAgg() extends  AggregateFunction[(String,Long ), Long , Long ]{
    override def createAccumulator(): Long =  0L

    override def add(value: (String, Long), accumulator: Long): Long = accumulator + 1

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long =  a + b
  }
}



//in out key window
class MarktingCountTotal() extends  WindowFunction[Long , MarketingViewCount, String , TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[MarketingViewCount]): Unit = {
    //去重 一个用户一直点 点了好几次的话
    val startTs =  new Timestamp(window.getStart).toString
    val endTs = new Timestamp(window.getEnd).toString
    val count = input.iterator.next()
    //去重也是比较简单的 但是数据量太大的时候需要使用布隆过滤器
    out.collect(MarketingViewCount(startTs,endTs,"app marketing ","total",count))
  }

}
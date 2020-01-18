package com.mlz.networkflowanalysis

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/*
 * @创建人: MaLingZhao
 * @创建时间: 2020/1/17
 * @描述： 
 */


//输入数据样例类
case class ApacheLogEvent(ip: String, userid: String, eventTime: Long, method: String, url: String)

//窗口聚合结果样例类
case class UrlViewCount(url: String, windowEnd: Long, count: Long)


object NetWorkFlow {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream = env.readTextFile("D:\\workspace\\bigdata\\flink\\UserBehaviorAnalysis\\NetWorkFlowAnalysis\\src\\main\\resources\\apache.log")
      .map(data => {
        val dataArray = data.split(" ")
        //定义时间转换
        val simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val timestamp = simpleDateFormat.parse(dataArray(3).trim).getTime
        ApacheLogEvent(dataArray(0).trim, dataArray(1).trim, timestamp, dataArray(5).trim, dataArray(6).trim)
      })
      //直接使用升序生成watermark的方法    一般的乱序使用BounedOutOf 周期性生成watermark的方法 默认是200ms
      //可以在env中进行设置

      // 实时性 迟到的数据的处理  正确性的处理 给多大的延迟是适合的   实际生产过程当中不应该有特别大的延迟 本身的延迟不是很大的时候
      //先给1s和2s的延迟  其他的在窗口进行聚合
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(1)) {
      // * 1000 ? 传进来的数据是s还是ms
      // s * 1000 ms 不*
      override def extractTimestamp(element: ApacheLogEvent): Long = element.eventTime
    })
      .keyBy(_.url)
      //统计10分钟的 5s钟进行一次滑窗
      .timeWindow(Time.minutes(10), Time.seconds(5))
      //允许60s的迟到数据
      .allowedLateness(Time.seconds(60))
      //聚合
      .aggregate(new CountAgg(), new WindowResult())

      //基于windowEnd分组
      .keyBy(_.windowEnd)
      .process(new TopNHotUrls(5))

    dataStream.print()
    env.execute("network flow job ")
  }
}

//输入 状态 累加器
//自定义预聚合函数
class CountAgg() extends AggregateFunction[ApacheLogEvent, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(value: ApacheLogEvent, accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

//自定义窗口处理函数
class WindowResult() extends WindowFunction[Long, UrlViewCount, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
    out.collect(UrlViewCount(key, window.getEnd, input.iterator.next()))
  }
}


//自定义排序输出处理函数
class TopNHotUrls(topSize: Int) extends KeyedProcessFunction[Long, UrlViewCount, String] {
  lazy val urlState: ListState[UrlViewCount] = getRuntimeContext.getListState(new ListStateDescriptor[UrlViewCount]("urlState", classOf[UrlViewCount]))

  override def processElement(value: UrlViewCount, context: KeyedProcessFunction[Long, UrlViewCount, String]#Context, collector: Collector[String]): Unit = {

    urlState.add(value)
    context.timerService().registerEventTimeTimer(value.windowEnd + 1)

  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {

    //从状态中拿到数据
    val allUrlView: ListBuffer[UrlViewCount] = new ListBuffer[UrlViewCount]()
    val iter = urlState.get().iterator()
    while (iter.hasNext) {
      allUrlView += iter.next()
    }
    urlState.clear()

    //按照count值从高到低进行降序排列
    val sortedUrlViews = allUrlView.sortWith(_.count > _.count).take(topSize)

    //格式化结果输出
    val result: StringBuilder = new StringBuilder()

    result.append("时间， ").append(new Timestamp(timestamp - 1)).append("\n")
    for (i <- sortedUrlViews.indices) {
      val currentUrlView = sortedUrlViews(i)
      result.append("Number").append(i+1) .append(":")
        .append("URL=").append(currentUrlView.url).append("\t")
        .append("访问量=").append(currentUrlView.count).append("\n")
    }
    result.append("============================")
    Thread.sleep(1000)

    out.collect(result.toString())

  }
}
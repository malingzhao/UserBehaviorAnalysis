package com.mlz.marketAnalysis

import java.sql.Timestamp
import java.util.UUID
import java.util.concurrent.TimeUnit

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

/*
 * @创建人: MaLingZhao
 * @创建时间: 2020/1/17
 * @描述： 
 */

/*
behavior 安装 卸载
channel  推广渠道   网页

 */
//输入数据样例类
case class MarketingUserBehavior(userId: String, behavior: String, channel: String, timestamp: Long)


//输出结果样例类
case class MarketingViewCount(windowStart:String , windowEnd:String ,
                              channel:String ,behavior:String ,count :Long  )



object APPMaektingByChannel {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


    val dataStream = env.addSource(new SimulatedEventSource())
      .assignAscendingTimestamps(_.timestamp)
      .filter(_.behavior != "UNINSTALL")
      .map(data=>{
        ((data.channel,data.behavior),1L)
      })
      .keyBy(_._1)      //以渠道和行为类型作为key分组
      .timeWindow(Time.hours(1), Time.seconds(10))
      .process(new MarjetingCountByChannel())

    dataStream.writeAsCsv("d:/bb.csv")
    //dataStream.print()
    dataStream.print()

    env.execute("app marketing by channel job ")
  }
}

//自定义数据源
class SimulatedEventSource() extends RichSourceFunction[MarketingUserBehavior] {
  // 标志位 判断是否正常执行   Running    定义是否运行的标志位 变量 var
  var running = true

  //定义用户行为的集合
  val behaviorType: Seq[String] = Seq("CLICK", "DOWNLOAD", "INSTALL", "UNINSTALL")

  //定义渠道的集合
  var channelSets: Seq[String] = Seq("wechat", "weibo", "appstore", "huaweistore")

  //定义一个随机数发生器
  val rand: Random = new Random()


  //生成我们想要的数据
  override def run(ctx: SourceFunction.SourceContext[MarketingUserBehavior]): Unit = {
    //定义一个生成数据的上限
    val maxElements = Long.MaxValue
    var  count = 0L

    // 随机生成所有数据
    while (running && count < maxElements) {

      val id = UUID.randomUUID().toString
      val behavior = behaviorType(rand.nextInt(behaviorType.size))
      val chanel = channelSets(rand.nextInt(channelSets.size))
      //当前的系统时间
      val ts = System.currentTimeMillis()

      ctx.collect(MarketingUserBehavior(id, behavior, chanel, ts))
      count += 1
      //睡眠
      TimeUnit.MICROSECONDS.sleep(10L)
    }


  }

  override def cancel(): Unit = {
    running = false
  }

}

//实现自定义的处理函数 In Out Key  T<:Window
case class MarjetingCountByChannel() extends
  ProcessWindowFunction[((String,String),Long), MarketingViewCount,(String,String),TimeWindow]{
  override def process(key: (String, String), context: Context, elements: Iterable[((String, String), Long)], out: Collector[MarketingViewCount]): Unit = {
    //去重 一个用户一直点 点了好几次的话
    val startTs =  new Timestamp(context.window.getStart).toString
    val endTs = new Timestamp(context.window.getEnd).toString
    val channel = key._1
    val behavior = key._2
    val count = elements.size

    //去重也是比较简单的 但是数据量太大的时候需要使用布隆过滤器
      out.collect(MarketingViewCount(startTs,endTs,channel,behavior,count))

  }


}

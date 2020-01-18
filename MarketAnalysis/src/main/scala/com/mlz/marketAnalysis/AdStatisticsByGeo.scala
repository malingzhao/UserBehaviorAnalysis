package com.mlz.marketAnalysis

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


/*
 * @创建人: MaLingZhao
 * @创建时间: 2020/1/17
 * @描述：  自定义ProcessFunction 实现相对复杂的黑名单过滤
 */


//输入的广告点击事件样例类
case class AdClickEvent(userId: Long, adId: Long, province: String, city: String, timestamp: Long)


//按照省份统计的输出结果样例类
case class CountByProvince(windowEnd: String, province: String, count: Long)


//输出的黑名单的报警信息 msg:报警信息
case class BlackListWarning(userId: Long, adId: Long, msg: String)

object AdStatisticsByGeo {
  //定义侧输出流的标签
  var blackListOutputTag: OutputTag[BlackListWarning] = new OutputTag[BlackListWarning]("blackList");

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    //val resource = getClass.getResource("./AdClickLog.csv")

    //读取数据并转换成AdClickEvent
    // val adEventStream = env.readTextFile(resource.getPath)
    val adEventStream = env.readTextFile("D:\\workspace\\bigdata\\flink\\UserBehaviorAnalysis\\MarketAnalysis\\src\\main\\scala\\com\\log\\log.csv")
      .map(data => {
        val dataArray = data.split(",")
        AdClickEvent(dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim, dataArray(3).trim, dataArray(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)
    //自定义process function 过滤大量刷点击的行为
    /*
    不同的状态的划分 如何实现  KeyedProcessFunciton adid userid
    过滤的过程中 定义一个侧输出流   包装起来
    如何拿到测输出流
     */
    val filterBlackStream = adEventStream
      .keyBy(data => (data.userId, data.adId))
      .process(new FilterBlackListUser(3))

    //根据省份做分组，开窗聚合
    val adCountStream = adEventStream
      //多个key的时候怎么办
      .keyBy(_.province)
      .timeWindow(Time.hours(1), Time.seconds(5))
      .aggregate(new AdCountAgg(), new AdCountResult())

    adCountStream.print("count")

    filterBlackStream.getSideOutput(blackListOutputTag).print("blackList")
    env.execute("ad statistics job")
  }


  /*
  把广告点击量保存下来
  来了一个新的时候判断是否超过上限
  代码程序来大量点击的话 ， 产生的时间很快
  保存一个是否已经已经发送过黑名单的状态
   */
  //最大的点击个数  in (userid,adid) out:  K I O
  class FilterBlackListUser(maxCount: Int) extends KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent] {
    //定义状态，保存当前用户对当前广告的点击量
    //已经对userid 和 adid 做过keyby了  ，
    lazy val countState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("count state", classOf[Long]))
    //保存是否发送过黑名单的状态
    lazy val isSentBlackList: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("isSent status", classOf[Boolean]))
    //保存定时器触发的时间戳
    lazy val resetTimer: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("resettime-state", classOf[Long]))

    override def processElement(value: AdClickEvent, ctx: KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]#Context, out: Collector[AdClickEvent]): Unit = {
      //取出count状态
      val curCount = countState.value()
      //如果是第一次处理 注册定时器,每天00:00触发

      if (curCount == 0) {
        //毫秒数  得到的是最终的天数 得到的就是明天的时间戳
        val ts = ((ctx.timerService().currentProcessingTime() / (1000 * 60 * 60 * 24)) + 1) * (1000 * 60 * 60 * 24)
        resetTimer.update(ts)
        //注册定时器
        ctx.timerService().registerProcessingTimeTimer(ts)
      }

      //判断计数是否达到上限 如果达到的话 加入黑名单
      if (curCount >= maxCount) {
        // 判断是否发送过黑名单 ，只发送一次
        //如果没有发送过
        if (!isSentBlackList.value()) {
          isSentBlackList.update(true)
          //输出到侧输出流
          ctx.output(blackListOutputTag, BlackListWarning(value.userId, value.adId, "Click Over" + maxCount + "times today"))
        }
        return
      }
      //计数状态 +1 输出数据到主流
      countState.update(curCount + 1)
      out.collect(value)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]#OnTimerContext, out: Collector[AdClickEvent]): Unit = {
      //定时器触发的时候清空状态
      if (timestamp == resetTimer.value()) {
        isSentBlackList.clear()
        countState.clear()
        resetTimer.clear()
      }

    }
  }

}

class AdCountAgg() extends AggregateFunction[AdClickEvent, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(value: AdClickEvent, accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

class AdCountResult() extends WindowFunction[Long, CountByProvince, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[CountByProvince]): Unit = {
    out.collect(CountByProvince((new Timestamp(window.getEnd).toString), key, input.iterator.next()))
  }
}


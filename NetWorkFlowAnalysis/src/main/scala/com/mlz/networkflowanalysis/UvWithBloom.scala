package com.mlz.networkflowanalysis

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

/*
 * @创建人: MaLingZhao
 * @创建时间: 2020/1/17
 * @描述： 
 */


object UvWithBloom {

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
      .filter(_.behavior == "pv") //只统计pv操作
      .map(data => ("dummyKey", data.userId))
      .keyBy(_._1)
      //开窗 1小时
      .timeWindow(Time.hours(1))
      //窗口函数 窗口关闭的时候调用redis的链接
      //窗口把数据存下来了
      //触发窗口的操作 触发器
      .trigger(new MyTrigger())
      .process(new UvCountWithBloom())
    dataStream.print()
    env.execute("uv  with bloom job ")
  }

}


//自定义的窗口触发器
class MyTrigger() extends Trigger[(String, Long), TimeWindow] {
  //每次来一个元素要做到额操作
  override def onElement(t: (String, Long), l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = {
    //每次来一条数据就直接触发窗口操作并清空所有的窗口状态 还可以有其他的操作 进行相应的调节
    TriggerResult.FIRE_AND_PURGE
  }

  //Trigger的种类 事件时间语义的话EventTrigger 当前的watermark大于当前


  //处理的时间语义
  override def onProcessingTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  //
  override def onEventTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  //窗口关闭之后的首位的操作
  override def clear(w: TimeWindow, triggerContext: Trigger.TriggerContext): Unit = {

  }
}


//定义一个布隆过滤器 几十个G的数据 压缩成几十M
class Bloom(size: Long) extends Serializable {
  // 位图的总大小 给默认的大小是 2 ^ 27 1的后面是27个0
  private val cmp = if (size > 0) size else 1 << 27

  //定义hash函数
  def hash(value: String, seed: Int): Long = {
    //保存结果
    var result = 0L
    /*
    如何算hash呢
       根据每一个字符的编码值按照某种算法叠加得到result
     */
    //for循环遍历 应用一定的算法在原有的result上做叠加
    for (i <- 0 until value.length) {
      result = result * seed + value.charAt(i)
    }
    //result是一个Long类型 但是我们是想要的是最后的27位的结果 所以我们可以做一个&运算
    result & (cmp - 1)
  }
}

/*

* */

class UvCountWithBloom() extends ProcessWindowFunction[(String, Long), UvCount, String, TimeWindow] {

  //定义redis的连接 不能频繁的打开和关闭
  lazy val jedis = new Jedis("localhost", 6379)
  //这个数大概是2的9次方M 就是512M 512位的存储空间  512/8 = 64M  大小的位图
  lazy val bloom = new Bloom(1 << 20)

  override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[UvCount]): Unit = {
    // 位图的存储方式是什么样子的 每一个窗口共用一个位图
    //每个窗口是自己单独的一个空间的表示 以窗口的某个时间戳作为key 那么value就是位图
    //bitmap

    //转成字符串
    val storeKey = context.window.getEnd.toString

    //UV统计  计数到底是多少 作为一个状态存在内存里面 每次来一次就清空一次 如果哦们想持续的获取的话
    var count = 0L

    //修改count
    //把每个窗口的uvcount值也存入redis， 所以要先从redis中读取
    //redis的count 只有一个值吗  一个窗口都应该有一个count值   应该按照不同的时间进行存储
    //我们应该把它存成一张表 ， 存放内容为(windowEnd->uvcount)
    //表的名称是
    if (jedis.hget("count", storeKey) != null) {
      count = jedis.hget("count", storeKey).toLong
    }

    //用布隆过滤器判断当前用户是否已经存在
    //拿出来最后一个数
    val userId = elements.last._2.toString
    //算出hash 如果是0 算出的hash就是在位图中的随机数
    val offset = bloom.hash(userId, 61)
    //定义一个标志位， 判断redis位图中有没有这一位
    //getbits 直接取出保存的哪一位 我们要的是 key 传进去 offset传进去
    val isExists = jedis.getbit(storeKey, offset)
    if (!isExists) {
      //如果不存在，位图对应位置1， 1 count = count + 1
      jedis.setbit(storeKey, offset, true)
      jedis.hset("count", storeKey, (count + 1).toString)
      out.collect(UvCount(storeKey.toLong, count + 1))
    } else {
      //数量保持不变
      out.collect(UvCount(storeKey.toLong, count))
    }
  }
}


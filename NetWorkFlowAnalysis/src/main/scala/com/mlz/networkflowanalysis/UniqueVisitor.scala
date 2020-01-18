package com.mlz.networkflowanalysis

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/*
 * @创建人: MaLingZhao
 * @创建时间: 2020/1/17
 * @描述： 
 */



case class  UvCount(windowEnd:Long, uvcount:Long )

object UniqueVisitor {
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
      //1小时的滚动时间窗口
      .timeWindowAll(Time.hours(1))
      //可以定义预聚合函数
      .apply(new UvCountByWindow())

    dataStream.print()


    env.execute("uv  job ")

  }
}

class UvCountByWindow() extends AllWindowFunction[UserBehavior, UvCount,TimeWindow]{
  override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[UvCount]): Unit = {
    //定义一个scala set,  用于保存所有数据并去重
    var idSet = Set[Long]()

    //把当前窗口的所有数据的id收集到set中 ，最后输出set的大小
    for(userBehavior<-input){
      //把每一个的id添加进去
      idSet += userBehavior.userId
    }
    //内存不够的话 放到redis做内存 出现极端的情况的话 我们使用别的方法 布隆过滤器
    out.collect(UvCount(window.getEnd , idSet.size))
  }
}

//去重的结果之后
//发现去重效果是很明显的
/*


 */

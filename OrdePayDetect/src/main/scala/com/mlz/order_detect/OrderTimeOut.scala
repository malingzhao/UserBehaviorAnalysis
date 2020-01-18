package com.mlz.order_detect


import java.util

import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/*
 * @创建人: MaLingZhao
 * @创建时间: 2020/1/18
 * @描述： 
 */


//定义输入 订单事件的样例类
case class OrderEvent(orderId: Long, eventType: String, txId: String, eventTime: Long)

//定义输出结果杨丽丽诶
case class OrderResult(orderId: Long, result: String)

object OrderTimeOut {
  def main(args: Array[String]): Unit = {


    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    //读取订单数据
    //val redource = getClass.getResource("pay.csv")
    val orderEventStream = env.readTextFile("D:\\workspace\\bigdata\\flink\\UserBehaviorAnalysis\\OrdePayDetect\\src\\main\\resources\\pay.csv")
     // val orderEventStream= env.socketTextStream("localhost",7777)
      .map(data => {
        val dataArray = data.split(",")
        OrderEvent(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong)
      })
      //分配事件时间
      .assignAscendingTimestamps(_.eventTime)
      //预先定义的处理
      .keyBy(_.orderId)


    /*
    只要匹配了 中间可以有别的事件插入进来
     */

    //2.定义一个匹配模式
    val orderPayPattern = Pattern.begin[OrderEvent]("begin").where(_.eventType == "create")
      .followedBy("follow").where(_.eventType == "pay")
      .within(Time.minutes(15)) //15分钟


    //3. 把模式应用到stream上，得到一个patternStream
    val patternStream = CEP.pattern(orderEventStream, orderPayPattern)

    //4. 调用select方法 ，提取事件序列 更加关心的是超时还没有支付成功的事件
    //create 检测到了 但是15分钟还没有pay的 notfollowed不能作为结束的状态 强调的是在两个状态之间
    //不能用notfollowedBy按照

    //超时的事件要做报警提示
    val orderTimeOutputTag = new OutputTag[OrderResult]("orderTimeOut")

    /*
    不同的实现方法
    传递参数 超时事件序列存放在map里面
    outputTag做一个明确的指令

    自定义的PatternTimeSout
    自定义的PatternSelect
     */

    val resultStream = patternStream.select(orderTimeOutputTag, new OrderTimeOutSelect(),
      new OrderPaySelect())
    resultStream.getSideOutput(orderTimeOutputTag).print("timeout")
    env.execute("order time out job ")
  }
}

//自定义超时序列事件的处理函数
//输入 输出
class OrderTimeOutSelect() extends PatternTimeoutFunction[OrderEvent, OrderResult] {
  //timeoutTimestamp超时的时间戳
  override def timeout(map: util.Map[String, util.List[OrderEvent]], timeoutTimestamp: Long): OrderResult = {
    val timeoutOrderId = map.get("begin").iterator().next().orderId
    //
    OrderResult(timeoutOrderId, "timeout")
  }
}

//自定义正常支付事件序列处理函数
class OrderPaySelect() extends PatternSelectFunction[OrderEvent, OrderResult] {
  override def select(map: util.Map[String, util.List[OrderEvent]]): OrderResult = {

    val payedOrderId = map.get("follow").iterator().next().orderId

    OrderResult(payedOrderId, "payed successfully")
  }
}
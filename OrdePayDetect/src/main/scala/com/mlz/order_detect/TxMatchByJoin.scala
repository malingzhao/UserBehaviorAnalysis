package com.mlz.order_detect

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/*
 * @创建人: MaLingZhao
 * @创建时间: 2020/1/18
 * @描述： 
 */


object TxMatchByJoin {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


    //读取订单事件流
    //val redource = getClass.getResource("pay.csv")
    val orderEventStream = env.readTextFile("D:\\workspace\\bigdata\\flink\\UserBehaviorAnalysis\\OrdePayDetect\\src\\main\\resources\\pay.csv")
      // val orderEventStream= env.socketTextStream("localhost",7777)
      .map(data => {
      val dataArray = data.split(",")
      OrderEvent(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong)
    })
      .filter(_.txId != "")
      //分配事件时间
      .assignAscendingTimestamps(_.eventTime)
      //预先定义的处理
      .keyBy(_.txId)


    //读取支付到账的事件流
    //val receiptResource = getClass.getResource("/")
    val receiptEventStream = env.readTextFile("D:\\workspace\\bigdata\\flink\\UserBehaviorAnalysis\\OrdePayDetect\\src\\main\\resources\\tx.csv")
      .map(data => {
        val dataArray = data.split(",")
        ReceiptEvent(dataArray(0).trim, dataArray(1).trim, dataArray(2).trim.toLong)
      })
      //升序
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.txId)

    //并不能找到只有它没有对应的一个的事件
    //join处理
    val processedStream = orderEventStream.intervalJoin(receiptEventStream)
      .between(Time.seconds(-5), Time.seconds(5))
      .process(new TxMatchByJoin())

    processedStream.print()
    env.execute("tx pay match by join job")
  }

  class TxMatchByJoin() extends ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)] {
    override def processElement(left: OrderEvent, right: ReceiptEvent, ctx: ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit ={
      out.collect((left,right))
    }

  }
}

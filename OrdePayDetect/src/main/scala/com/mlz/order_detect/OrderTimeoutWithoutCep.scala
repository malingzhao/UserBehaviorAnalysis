package com.mlz.order_detect

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/*
 * @创建人: MaLingZhao
 * @创建时间: 2020/1/18
 * @描述： 
 */


object OrderTimeoutWithoutCep {
  val orderTimeOutputTag = new OutputTag[OrderResult]("orderTimeout")

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

    //定义一个process function进行超时检测
    //val timeoutWarningStream = orderEventStream.process(new OrderTimeOutWarning())

    val orderResultStream = orderEventStream.process(new OrderPayMatch())
    orderResultStream.getSideOutput(orderTimeOutputTag).print("timeout")

    env.execute("order time out without  cep job")
  }

  class OrderPayMatch() extends KeyedProcessFunction[Long, OrderEvent, OrderResult] {
    lazy val isPayedState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor("ispayed state", classOf[Boolean]))

    //保存定时器的时间戳为状态
    lazy val timeState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor("timer- tate", classOf[Long]))


    override def processElement(value: OrderEvent, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context, out: Collector[OrderResult]): Unit = {

      val isPayed = isPayedState.value()
      val timerTs = timeState.value()

      //根据事件的类型进行分类判断 做不同的处理逻辑
      if (value.eventType == "create") {
        //1 如果是create事件 判断pay是否来过

        if (isPayed) {
          // 1.1 如果已经配过 那么匹配成功 输主流数据 清空状态  先来pay 再来的create 超过15分那种
          out.collect(OrderResult(value.orderId, "payed successfully"))
          ctx.timerService().deleteEventTimeTimer(timerTs)
          isPayedState.clear()
          timeState.clear()
        } else {
          //1.2 如果没有pay过注册定时器等待pay的到来
          val ts = value.eventTime * 1000 + 15 * 60 * 1000
          ctx.timerService().registerEventTimeTimer(ts)
          timeState.update(ts)
        }

      } else if (value.eventType == "pay") {
        //2 如果是pay事件，那么判断create是否create过 用timer表示
        if (timerTs > 0) {
          //2.1 如果有定时器说明已经有create来过

          //继续判断是否超过了timeout时间
          /*
          判断当前pay的时间和create的时间相比是不是在15分钟以上
          * */


          if (timerTs > value.eventTime * 1000L) {
            //2.1.1 如果定时器的时间还没到，那么输出成功匹配
            out.collect(OrderResult(value.orderId, "payed successfully"))
          } else {
            //2.1.2 如果当前pay已经超时，那么输出到侧输出流
            ctx.output(orderTimeOutputTag,OrderResult(value.orderId,"payed but already timeout"))
          }

          //输出结束清空状态
          ctx.timerService().deleteEventTimeTimer(timerTs)
          isPayedState.clear()
          timeState.clear()
        }else{
          //2.2 pay先到了 更新状态,注册定时器 等待create
          isPayedState.update(true)
          //根据什么注册 为什么会出现pay先来 create 还没到 是因为乱序 等到watermark涨到这个时间戳
          //watermark
          ctx.timerService().registerEventTimeTimer(value.eventTime * 1000L)
          timeState.update(value.eventTime * 1000L)
        }
      }
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#OnTimerContext,
                         out: Collector[OrderResult]): Unit ={
    //根据状态的值 ，判断哪个数据没来
      if(isPayedState.value()){
      //如果为trur表示pay先到了
      ctx.output(orderTimeOutputTag,OrderResult(ctx.getCurrentKey,"already payed but not found create "))
    }else{
      //表示create到了，没等到pay
      ctx.output(orderTimeOutputTag,OrderResult(ctx.getCurrentKey,"order timeout"))
    }
      isPayedState.clear()
      timeState.clear()

    }
  }
}


//实现自定义的处理函数
class OrderTimeOutWarning() extends KeyedProcessFunction[Long, OrderEvent, OrderResult] {
  //保存pay支付事件是否来过的状态
  lazy val isPayedState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor("ispayed state", classOf[Boolean]))


  override def processElement(value: OrderEvent, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context, out: Collector[OrderResult]): Unit = {
    //先取出状态标志位
    val isPayed = isPayedState.value()

    if (value.eventType == "create" && !isPayed) {
      //如果遇到了create事件，并且key没有来过，注册定时器 开始等待
      ctx.timerService().registerEventTimeTimer(value.eventTime * 1000L + 15 * 60 * 1000L)
    } else if (value.eventType == "pay") {
      //如果是pay事件,直接把状态改成true
      isPayedState.update(true)
    }
  }

  //定时器触发
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#OnTimerContext, out: Collector[OrderResult]): Unit = {
    //直接判断isPayed状态是否为true
    val isPayed = isPayedState.value()

    if (isPayed) {
      out.collect(OrderResult(ctx.getCurrentKey, "order payed successfully"))
    } else {
      out.collect(OrderResult(ctx.getCurrentKey, "order  timeout"))
    }
    //清空状态
    isPayedState.clear()
  }
}
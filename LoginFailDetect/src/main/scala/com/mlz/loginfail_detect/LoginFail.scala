package com.mlz.loginfail_detect

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer


/*
 * @创建人: MaLingZhao
 * @创建时间: 2020/1/18
 * @描述：
 */

//输入的登录事件样例类
case class LoginEvent(userId: Long, ip: String, evrntType: String, eventTime: Long)


//输出的异常报警信息样例类
case class Warning(userId: Long, firstFailTime: Long, lastFailTime: Long, warningMsg: String)


object LoginFail2 {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env.setParallelism(1)



    /*
    可能会存在
    */

    //读取事件数据
    //val resource = getClass.getResource("D:\\workspace\\bigdata\\flink\\UserBehaviorAnalysis\\LoginFailDetect\\src\\main\\scala\\com\\mlz\\loginfail_detect\\logfail.csv")

    val logingFialEventStream = env.readTextFile("D:\\workspace\\bigdata\\flink\\UserBehaviorAnalysis\\LoginFailDetect\\src\\main\\scala\\com\\mlz\\loginfail_detect\\logfail.csv")
      .map(data => {
        val dataArray = data.split(",")
        LoginEvent(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(5)) {
        override def extractTimestamp(element: LoginEvent): Long = 1000L
      })


    val warningStream = logingFialEventStream
      .keyBy(_.userId) //用户id做分组
      .process(new LoginWarning(2))
    warningStream.print()
    env.execute("login fail detect job")
  }
}

/*
ip 具体情况具体分析 只拿到了ip  正常的话 测试每一个用户的登录
用户ip做分组不是一个很好的选择

实现复杂的功能呢需要 底层API

flink
* */

//keyby之后的  K I  O
class LoginWarning(maxFailItems: Int) extends KeyedProcessFunction[Long, LoginEvent, Warning] {

  //定注意状态 保存两s内的所有失败事件
  lazy val loginFailState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("login fail status ", classOf[LoginEvent]))


  override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#Context, out: Collector[Warning]): Unit = {
    //    //来一个失败就放到list里面 什么时候去判断呢 如果判断注册定时器 第一次注册定时器 然后加
    //    //然后判断 如果有登录成功的话 那么就清空状态
    //
    //
    //    val loginFailList = loginFailState.get()
    //    //判断类型是否是fail 制度去fail的事件到状态
    //    //登录成功  也是有相对应的操作的
    //    if (value.evrntType == "fail") {
    //      //判断是否有值
    //      if(!loginFailList.iterator().hasNext){
    //        ctx.timerService().registerEventTimeTimer(value.eventTime * 1000L +  2000L)
    //      }
    //      loginFailState.add(value)
    //    }else{
    //      //如果是成功，清空状态
    //      loginFailState.clear()
    //    }

    if (value.evrntType == "fail") {
      //如果是失败的话，判断之前是否有登录失败事件
      val iter = loginFailState.get().iterator()
      if (iter.hasNext) {
        //如果已经有登录失败事件 ，就比较事件的时间
        val firstFail = iter.next()
        //在两s中之内
        if (value.eventTime < firstFail.eventTime + 2) {
          //如果两次间隔小于2s， 输出报警
          out.collect(Warning(value.userId, firstFail.eventTime, value.eventTime, "login fail in 2 seconds"))
        }
        //更新最近一次的登录失败事件,保存在状态里面面
        loginFailState.clear()
        loginFailState.add(value)

      }else{
        // 如果是第一次登录失败，直接添加到状态
        loginFailState.add(value)
      }
    } else {
      //如果是成功的话 清空状态
      loginFailState.clear()

    }
  }

  //  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#OnTimerContext, out: Collector[Warning]): Unit ={
  //    //触发定时器的时候， 根据状态里的失败个数决定是否输出报警
  //    val allLoginFials:ListBuffer[LoginEvent]=new ListBuffer[LoginEvent]()
  //    val iter = loginFailState.get().iterator()
  //    while (iter.hasNext){
  //      allLoginFials += iter.next()
  //    }
  //    //判断个数
  //    if(allLoginFials.length >= maxFailItems){
  //      out.collect(Warning(allLoginFials.head.userId, allLoginFials.head.eventTime, allLoginFials.last.eventTime, "login fail in 2 seconds\t"+ allLoginFials.length +"\ttimes" ))
  //    }
  //
  //    //清空状态
  //    loginFailState.clear()
  //
  //  }


}
/*
maxFailTime 没有起到太大的作用
1个小时的乱序时间
如果想要处理乱序数据的话
是非常麻烦的
实际上
如何处理
把watermark机制利用起来 不是直接+2的操作
写一个定时器 watermark做延迟之后的
一个数据来了之后 判断是不是真正的登录失败

连续3次 连续10次

我们自己实现那么多逻辑的话 是很难实现的

CEP 是专门处理复杂事件等相关的CEP的问题的

*
*/

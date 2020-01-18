package com.mlz.lianxi.source

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer



/*
 * @创建人: MaLingZhao
 * @创建时间: 2020/1/17
 * @描述： 
 */

//定义样例类
case class SensorReading(id:String, timestamp:Long, tempeature: Double)


object KafkaTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 1. 从自定义的集合中读取数据
    val stream1 = env.fromCollection( List(
      SensorReading("sensor_1", 1547718199, 35.80018327300259),
      SensorReading("sensor_6", 1547718201, 15.402984393403084),
      SensorReading("sensor_7", 1547718202, 6.720945201171228),
      SensorReading("sensor_10", 1547718205, 38.101067604893444)
    ) )

    //    env.fromElements( 1, 2.0, "string" ).print()

    // 2. 从文件中读取数据
    // val stream2 = env.readTextFile("D:\\Projects\\BigData\\FlinkTutorial\\src\\main\\resources\\sensor.txt")

    //  3. 从kafka中读取数据
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    val stream3 = env.addSource( new FlinkKafkaConsumer[String]("sensor", new SimpleStringSchema(), properties) )



    stream3.print("stream4").setParallelism(1)

    env.execute("source test")
  }

}

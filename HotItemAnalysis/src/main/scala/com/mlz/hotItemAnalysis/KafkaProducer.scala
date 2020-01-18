package com.mlz.hotItemAnalysis

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/*
 * @创建人: MaLingZhao
 * @创建时间: 2020/1/17
 * @描述： 
 */


object KafkaProducer {

  def main(args: Array[String]): Unit = {
    writeToKafka("hotitems")
  }

  def writeToKafka(topic: String): Unit = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "consumer-group")

    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")


    //定义一个kafka producer
    val producer = new KafkaProducer[String, String](properties)
    //从文件中读取数据发送
    val bufferedSource = io.Source.fromFile("D:\\workspace\\bigdata\\flink\\UserBehaviorAnalysis\\HotItemAnalysis\\src\\main\\resources\\UserBehavior.csv")

    for (line <-bufferedSource.getLines()){
      val record = new ProducerRecord[String,String](topic, line )
      producer.send(record)
    }
    producer.close()

  }
}

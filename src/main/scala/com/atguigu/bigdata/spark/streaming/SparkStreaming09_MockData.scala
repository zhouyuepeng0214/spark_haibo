package com.atguigu.bigdata.spark.streaming

import java.util.{Properties, Random}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

object SparkStreaming09_MockData {

  def main(args: Array[String]): Unit = {

    val properties = new Properties()
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop110:9092")
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String,String](properties)

    while (true) {
      for (i <- 0 until  new Random().nextInt(50))  {
        val time: Long = System.currentTimeMillis()

        producer.send(new ProducerRecord[String,String]("kafka",time.toString))
        println(time)
      }

      Thread.sleep(2000)
    }

  }

}

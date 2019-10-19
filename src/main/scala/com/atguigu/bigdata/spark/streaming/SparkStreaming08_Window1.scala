package com.atguigu.bigdata.spark.streaming

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming08_Window1 {

  def main(args: Array[String]): Unit = {


    val conf: SparkConf = new SparkConf().setAppName("SparkStreaming08_Window1").setMaster("local[*]")

    val ssc = new StreamingContext(conf, Seconds(5))

    val kafkaDStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream[String,String,StringDecoder,StringDecoder](
      ssc,
      Map(
        "zookeeper.connect" -> "hadoop110:2181",
        ConsumerConfig.GROUP_ID_CONFIG -> "atguiguKafkaGroup",
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer"
      ),
      Map(
        "kafka" -> 3
      ),
      StorageLevel.MEMORY_ONLY
    )

    val valueDStream: DStream[String] = kafkaDStream.map(_._2)

    val windowDStream: DStream[String] = valueDStream.window(Seconds(60),Seconds(10))

    val timeDstream: DStream[(String, Long)] = windowDStream.map(time => {
      val t: String = time.substring(0, time.length - 4) + "0000"
      (t, 1L)
    })
    val value: DStream[(String, Long)] = timeDstream.reduceByKey(_+_)

    value.print()

    //启动采集器
    ssc.start()

    //等待采集器的执行完毕
    ssc.awaitTermination()


  }

}

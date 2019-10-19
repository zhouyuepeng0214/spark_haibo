package com.atguigu.bigdata.spark.streaming

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming05_SourceKafka {

  def main(args: Array[String]): Unit = {


    val conf: SparkConf = new SparkConf().setAppName("SparkStreaming05_WordCount").setMaster("local[*]")

    val ssc = new StreamingContext(conf,Seconds(3))

    val kafkaDStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
      ssc,
      Map(
        "zookeeper.connect" -> "hadoop110:2181",
        ConsumerConfig.GROUP_ID_CONFIG -> "atguiguKafkaGroup",
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer"
      ),
      Map(
        // todo kafa 的topic及对应的分区数
        "kafka" -> 3
      ),
      StorageLevel.MEMORY_ONLY
    )

    val valueDStream: DStream[String] = kafkaDStream.map(_._2)

    valueDStream.print()

    val word: DStream[String] = valueDStream.flatMap(_.split(" "))

    val wordtoone: DStream[(String, Int)] = word.map((_,1))

    val wordcount: DStream[(String, Int)] = wordtoone.reduceByKey(_+_)

    // todo foreachRDD
    wordcount.foreachRDD(rdd => {
      rdd.foreach(println)
    })

    //启动采集器
    ssc.start()

    //等待采集器的执行完毕
    ssc.awaitTermination()


  }

}

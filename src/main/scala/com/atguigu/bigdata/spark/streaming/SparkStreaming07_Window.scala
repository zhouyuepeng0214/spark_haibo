package com.atguigu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming07_Window {

  def main(args: Array[String]): Unit = {


    val conf: SparkConf = new SparkConf().setAppName("SparkStreaming07_Window").setMaster("local[*]")

    val ssc = new StreamingContext(conf, Seconds(3))

    /*
    val ints = List(1,2,3,4,5,6,7)

    val intses: Iterator[List[Int]] = ints.sliding(3,2)

    for (elem <- intses) {
      println(elem)
    }
    */

    val lineDStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop110", 9999)

    //val windowDStream: DStream[String] = lineDStream.window(Seconds(6), Seconds(3))

    val wordDStream: DStream[String] = lineDStream.flatMap(line => line.split(" "))

    val wordToOneDStream: DStream[(String, Long)] = wordDStream.map {
      word => (word, 1L)
    }

    val value: DStream[(String, Long)] = wordToOneDStream.reduceByKeyAndWindow((a:Long,b:Long) => (a + b),Seconds(6), Seconds(3))

    value.print()


    //启动采集器
    ssc.start()

    //等待采集器的执行完毕
    ssc.awaitTermination()


  }

}

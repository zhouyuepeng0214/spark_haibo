package com.atguigu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming03_SourceFile {

  def main(args: Array[String]): Unit = {


    val conf: SparkConf = new SparkConf().setAppName("SparkStreaming01_WordCount").setMaster("local[*]")

    val ssc = new StreamingContext(conf,Seconds(3))

    val lineDStream: DStream[String] = ssc.textFileStream("in")

    val wordDStream: DStream[String] = lineDStream.flatMap(line => line.split(" "))

    val wordToOneDStream: DStream[(String, Long)] = wordDStream.map {
      word => (word, 1L)
    }

    val wordToCountDStream: DStream[(String, Long)] = wordToOneDStream.reduceByKey(_+_)

    wordToCountDStream.print()

    //释放资源
    //main方法不能执行完毕，因为一旦main方法执行完毕，环境对象就会被回收
//    ssc.stop()

    //启动采集器
    ssc.start()

    //等待采集器的执行完毕
    ssc.awaitTermination()


  }

}

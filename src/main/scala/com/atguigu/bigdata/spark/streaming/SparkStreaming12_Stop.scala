package com.atguigu.bigdata.spark.streaming

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}

object SparkStreaming12_Stop {

  def main(args: Array[String]): Unit = {


    val conf: SparkConf = new SparkConf().setAppName("SparkStreaming12_Stop").setMaster("local[*]")

    // todo 优雅的关闭设置
    conf.set("spark.streaming.stopGracefullyOnShutdown","true")

    val ssc = new StreamingContext(conf,Seconds(3))

    val lineDStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop110",9999)

    val wordDStream: DStream[String] = lineDStream.flatMap(line => line.split(" "))

    val wordToOneDStream: DStream[(String, Long)] = wordDStream.map {
      word => (word, 1L)
    }

    val wordToCountDStream: DStream[(String, Long)] = wordToOneDStream.reduceByKey(_+_)

    wordToCountDStream.foreachRDD(rdd => rdd.foreach(println))

    // todo 优雅的关闭
    // 启动新的线程，希望在特殊的场合关闭SparkStreaming
    new Thread(new Runnable {
      override def run(): Unit = {
        while (true) {
          try {
            Thread.sleep(5000)
          } catch {
            case ex : Exception => println(ex)
          }
          // 监控HDFS文件的变化
          val fs: FileSystem = FileSystem.get(new URI("hdfs://hadoop110:9000"),new Configuration,"atguigu")
          val state: StreamingContextState = ssc.getState()
          // 如果环境对象处于活动状态，可以进行关闭操作
          if (state == StreamingContextState.ACTIVE) {
            val flg: Boolean = fs.exists(new Path("hdfs://hadoop110:9000/stopSpark"))
            if (flg) {
              // 关闭采集器和Driver:优雅的关闭
              ssc.stop(true,true)
              System.exit(0)
            }
          }
        }
      }
    }).start()

    //启动采集器
    ssc.start()

    //等待采集器的执行完毕
    ssc.awaitTermination()


  }

}

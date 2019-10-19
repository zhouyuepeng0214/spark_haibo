package com.atguigu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming10_Transform {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("SparkStreaming10_Transform").setMaster("local[*]")

    val ssc = new StreamingContext(conf, Seconds(3))

    val lineDStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop110", 9999)

    val wordDStream: DStream[String] = lineDStream.flatMap(line => line.split(" "))

    val wordToOneDStream: DStream[(String, Long)] = wordDStream.map {
      word => (word, 1L)
    }

    val value: DStream[(String, Long)] = wordToOneDStream.reduceByKey(_+_)

    //todo 位置Driver Coding (执行次数1)
    val value1: DStream[Long] = value.map {
      case (k, v) => {
        //todo 位置Executor Coding (执行次数)N
        v
      }
    }

    //todo 位置Driver Coding (执行次数1)
    value.transform{
      rdd => {
        // todo Driver Coding(M) 周期数
        rdd.map{
          case (k, v) => {
            //todo 位置Executor Coding (执行次数)N
            v
          }
        }
      }
    }


    //启动采集器
    ssc.start()

    //等待采集器的执行完毕
    ssc.awaitTermination()


  }

}

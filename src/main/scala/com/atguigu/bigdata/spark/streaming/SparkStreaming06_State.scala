package com.atguigu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming06_State {

  def main(args: Array[String]): Unit = {


    val conf: SparkConf = new SparkConf().setAppName("SparkStreaming06_State").setMaster("local[*]")

    val ssc = new StreamingContext(conf,Seconds(3))

    ssc.sparkContext.setCheckpointDir("input1")

    val lineDStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop110",9999)

    val wordDStream: DStream[String] = lineDStream.flatMap(line => line.split(" "))

    val wordToOneDStream: DStream[(String, Long)] = wordDStream.map {
      word => (word, 1L)
    }

//    val wordToCountDStream: DStream[(String, Long)] = wordToOneDStream.reduceByKey(_+_)

    // todo DStream 有状态操作时依赖于检查点完成的
    val stateDStream: DStream[(String, Long)] = wordToOneDStream.updateStateByKey[Long](
      (seq: Seq[Long], buffer: Option[Long]) => {
        val sum: Long = seq.sum + buffer.getOrElse(0L)
        Option(sum)
      }
    )

    stateDStream.print()

    // 释放资源
    // 流式数据处理当中，上下文环境对象不能停止的。
    // todo main方法不能执行完毕，因为一旦main放置执行完毕，环境对象就会被回收。
    //ssc.stop()

    //启动采集器
    ssc.start()

    //等待采集器的执行完毕
    ssc.awaitTermination()


  }

}

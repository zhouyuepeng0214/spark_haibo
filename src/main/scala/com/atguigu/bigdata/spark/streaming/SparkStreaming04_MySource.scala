package com.atguigu.bigdata.spark.streaming

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.control.Breaks

object SparkStreaming04_MySource {

  def main(args: Array[String]): Unit = {


    val conf: SparkConf = new SparkConf().setAppName("SparkStreaming01_WordCount").setMaster("local[*]")

    val ssc = new StreamingContext(conf,Seconds(3))

    val lineDStream: ReceiverInputDStream[String] = ssc.receiverStream(new CustomerReceiver("hadoop110",9999))

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

// 自定义采集器
// 1. 继承Receiver抽象类
// 2. 重写方法：onStart， onStop
class CustomerReceiver(host : String,port : Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) {
  override def onStart(): Unit = {
    new Thread("Socket Receiver") {
      override def run(): Unit = {
        receive()
      }
    }.start()
  }

  def receive(): Unit = {

    val socket = new Socket(host,port)

    val reader = new BufferedReader(
      new InputStreamReader(
        socket.getInputStream,
        "UTF-8"
      )
    )
    var s =""
    Breaks.breakable{
      while((s = reader.readLine()) != null) {
        if ("==END==".equals(s)) {
          Breaks.break()
        } else {
          store(s)
        }
      }
    }
  }

  override def onStop(): Unit = {

  }
}

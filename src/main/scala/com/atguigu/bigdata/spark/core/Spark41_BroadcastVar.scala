package com.atguigu.bigdata.spark.core

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark41_BroadcastVar {

  def main(args: Array[String]): Unit = {

    // 准备Spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")

    // 获取Spark上下文环境对象 :
    val sc = new SparkContext(conf)

    //

    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a",1),("b",2),("c",3)))
    //val rdd2: RDD[(String, Int)] = sc.makeRDD(List(("a",4),("b",5),("c",4)))

    val list = List(("a",4),("b",5),("c",4))

    //使用广播变量，改善性能问题
    // todo 广播变量的数据只在Executor中存储一份，所有的Task共享这个变量数据

    val broadcast: Broadcast[List[(String, Int)]] = sc.broadcast(list)

//    val rdd3: RDD[(String, (Int, Int))] = rdd1.join(rdd2)
//    rdd3.foreach(println)

    val rdd3: RDD[(String, (Int, Int))] = rdd1.map {
      case (k, v1) => {
        var v2 = 0
        for (elem <- broadcast.value) {
          if (k == elem._1) {
            v2 = elem._2
          }
        }
        (k, (v1, v2))
      }
    }
    rdd3.foreach(println)

    // 释放资源
    sc.stop()

  }
}


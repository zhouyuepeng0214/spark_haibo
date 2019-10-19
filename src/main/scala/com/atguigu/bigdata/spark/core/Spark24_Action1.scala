package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark24_Action1 {
  def main(args: Array[String]): Unit = {

    // 准备Spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")

    // 获取Spark上下文环境对象 :
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(5,1,2,3,4))

    val i: Int = rdd.reduce(_+_)
    //print(i)

    //rdd.collect()foreach(println)

    //println(rdd.count())

    //println(rdd.first())

    println(rdd.take(2).mkString("-"))
    // todo 先排序再取值
    println(rdd.takeOrdered(2).mkString("-"))

    // 释放资源
    sc.stop()

  }
}

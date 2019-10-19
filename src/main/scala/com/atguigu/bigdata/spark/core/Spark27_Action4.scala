package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark27_Action4 {
  def main(args: Array[String]): Unit = {

    // 准备Spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")

    // 获取Spark上下文环境对象 :
    val sc = new SparkContext(conf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a",1),("b",1),("a",1),("b",1)))

    val stringToLong: collection.Map[String, Long] = rdd.countByKey()

    println(stringToLong)

    val tupleToLong: collection.Map[(String, Int), Long] = rdd.countByValue()

    println(tupleToLong)

    // 释放资源
    sc.stop()

  }
}

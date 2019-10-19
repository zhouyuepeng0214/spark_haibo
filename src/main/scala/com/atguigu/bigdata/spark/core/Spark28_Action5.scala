package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark28_Action5 {
  def main(args: Array[String]): Unit = {

    // 准备Spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")

    // 获取Spark上下文环境对象 :
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6))

    // todo collect之后在driver打印
    rdd.collect().foreach(println)
    println("**********")
    // todo 算子前面的代码在 Driver Coding
    rdd.foreach{
      // todo 算子里面的代码在 Executor Coding 打印
      println}

    // 释放资源
    sc.stop()

  }
}


//Stage (阶段）的数量= 1 + shuffle的数量



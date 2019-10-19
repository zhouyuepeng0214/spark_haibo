package com.atguigu.bigdata.spark.core

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_WordCount1 {

  def main(args: Array[String]): Unit = {

    // 使用Spark框架完成第一个案例：WordCount

    // 准备Spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")

    // 获取Spark上下文环境对象 :
    val sc = new SparkContext(conf)

    sc.textFile("input").flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).collect().foreach(println)

    // 释放资源
    sc.stop()

  }
}

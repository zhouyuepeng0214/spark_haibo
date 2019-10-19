package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark21_RDD_Transform15 {
  def main(args: Array[String]): Unit = {
    // 使用Spark框架完成第一个案例：WordCount

    // 准备Spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")

    // 获取Spark上下文环境对象 :
    val sc = new SparkContext(conf)

    val rdd: RDD[(Int, String)] = sc.parallelize(Array((3,"aa"),(6,"cc"),(2,"bb"),(1,"dd")))

    // todo 默认true 升序，
    //val rdd1: RDD[(Int, String)] = rdd.sortByKey(false)

    val rdd1: RDD[(Int, String)] = rdd.mapValues(s => s + "5555")

    rdd1.collect().foreach(println)

    // 释放资源
    sc.stop()

  }
}

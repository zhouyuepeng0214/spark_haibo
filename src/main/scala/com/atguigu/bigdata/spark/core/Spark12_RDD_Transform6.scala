package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark12_RDD_Transform6 {
  def main(args: Array[String]): Unit = {
    // 使用Spark框架完成第一个案例：WordCount

    // 准备Spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")

    // 获取Spark上下文环境对象 :
    val sc = new SparkContext(conf)

    val distinctRDD: RDD[Int] = sc.makeRDD(List(1,2,1,5,2,9,6,1))

    //val distinctRDD: RDD[Int] = numRDD.distinct()
    // 分区数量等同于任务的数量
    val unionRDD: RDD[Int] = distinctRDD.distinct(3)

    unionRDD.collect().foreach(println)

    unionRDD.glom().collect().foreach(datas => {
      println("*********")
      datas.foreach(println)
    })

    // 释放资源
    sc.stop()

  }
}

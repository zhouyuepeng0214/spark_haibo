package com.atguigu.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark13_RDD_Transform7 {
  def main(args: Array[String]): Unit = {
    // 使用Spark框架完成第一个案例：WordCount

    // 准备Spark配置对象
    val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")

    // 获取Spark上下文环境对象 :
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(1 to 16,4)

    // todo coalesce 只能减少分区，不能增加
    //val coalesceRDD: RDD[Int] = rdd.coalesce(6)

    // todo 打乱重新分区
    val coalesceRDD: RDD[Int] = rdd.repartition(6)

    coalesceRDD.glom().collect()foreach(datas => {
      println("******")
      datas.foreach(println)
    })
    println(coalesceRDD.partitions.size)

    // 释放资源
    sc.stop()

  }
}
